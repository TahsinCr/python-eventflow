#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <math.h>
#include <pythread.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <structmember.h>

#if defined(_MSC_VER)
#include <intrin.h>
static inline uint8_t ef_atomic_load_u8(const uint8_t *ptr) {
    uint8_t val = *(const volatile uint8_t *)ptr;
    _ReadWriteBarrier();
    return val;
}
static inline void ef_atomic_store_u8(uint8_t *ptr, uint8_t val) {
    _InterlockedExchange8((volatile char *)ptr, (char)val);
}
static inline int64_t ef_atomic_load_i64(const int64_t *ptr) {
    return (int64_t)_InterlockedCompareExchange64((volatile __int64 *)ptr, 0, 0);
}
static inline void ef_atomic_add_i64(int64_t *ptr, int64_t delta) {
#if defined(_M_AMD64) || defined(_M_ARM64)
    _InterlockedExchangeAdd64((volatile __int64 *)ptr, (__int64)delta);
#else
    volatile __int64 *target = (volatile __int64 *)ptr;
    __int64 old_val, new_val;
    do {
        old_val = *target;
        new_val = old_val + delta;
    } while (_InterlockedCompareExchange64(target, new_val, old_val) != old_val);
#endif
}
#else
static inline uint8_t ef_atomic_load_u8(const uint8_t *ptr) {
    return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
}
static inline void ef_atomic_store_u8(uint8_t *ptr, uint8_t val) {
    __atomic_store_n(ptr, val, __ATOMIC_RELEASE);
}
static inline int64_t ef_atomic_load_i64(const int64_t *ptr) {
    return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
}
static inline void ef_atomic_add_i64(int64_t *ptr, int64_t delta) {
    __atomic_fetch_add(ptr, delta, __ATOMIC_SEQ_CST);
}
#endif

typedef struct {
    PyTypeObject *flag_type;
    PyTypeObject *subscriber_type;
    PyTypeObject *bus_core_type;
    PyTypeObject *op_stats_core_type;

    PyObject *publish_result_type;
    PyObject *descr_matched;
    PyObject *descr_delivered;
    PyObject *descr_failed;
    PyObject *descr_errors;
    PyObject *empty_publish_result;
    PyObject *empty_tuple;

    PyObject *event_bus_stats_type;
    PyObject *event_filter_type;
    PyObject *closed_error_type;
    PyObject *invalid_handler_error_type;
    PyObject *error_context_type;
    PyObject *exception_group_type;
    PyObject *threading_lock_fn;
    PyObject *rlock_type;

    PyObject *operation_metrics_type;
    PyObject *operation_stats_snapshot_type;
    PyObject *mapping_proxy_type;

    PyObject *is_awaitable_fn;
    PyObject *close_awaitable_fn;

    PyObject *zero_int;
    Py_hash_t h_other;

    PyObject *k__lock;
    PyObject *k__active;
    PyObject *k__consume;
    PyObject *k__subscriber_snapshot;
    PyObject *k__publish_python;
    PyObject *k_cancel;
    PyObject *k_close;
    PyObject *k_notify_all;
    PyObject *k_matches;
    PyObject *k_acquire;
    PyObject *k_release;
    PyObject *k_append;
    PyObject *k_clear;
    PyObject *k___enter__;
    PyObject *k___exit__;
    PyObject *k_success;
    PyObject *k_error;
    PyObject *k_cancelled;
    PyObject *k_other;
} CoreState;

static inline CoreState *get_core_state(PyObject *module) {
    return (CoreState *)PyModule_GetState(module);
}

static inline int ef_dict_get_item_ref(PyObject *dict, PyObject *key, PyObject **result) {
#if PY_VERSION_HEX >= 0x030D0000
    return PyDict_GetItemRef(dict, key, result);
#else
    PyObject *borrowed = PyDict_GetItemWithError(dict, key);
    if (borrowed != NULL) {
        *result = Py_NewRef(borrowed);
        return 1;
    }
    *result = NULL;
    return PyErr_Occurred() ? -1 : 0;
#endif
}

static inline int ef_is_non_awaitable_type(PyObject *obj) {
    PyTypeObject *tp = Py_TYPE(obj);
    return (
        tp == &PyBool_Type
        || tp == &PyLong_Type
        || tp == &PyFloat_Type
        || tp == &PyUnicode_Type
        || tp == &PyBytes_Type
        || tp == &PyByteArray_Type
        || tp == &PyComplex_Type
        || tp == &PyTuple_Type
        || tp == &PyList_Type
        || tp == &PyDict_Type
        || tp == &PySet_Type
        || tp == &PyFrozenSet_Type
    );
}

static int ef_check_not_awaitable(
    CoreState *st,
    PyObject *result,
    PyObject *error_type,
    const char *message
) {
    if (result == Py_None || ef_is_non_awaitable_type(result)) {
        return 0;
    }
    if (PyCoro_CheckExact(result)) {
        PyObject *closed = PyObject_CallOneArg(st->close_awaitable_fn, result);
        if (closed == NULL) {
            return -1;
        }
        Py_DECREF(closed);
        PyErr_SetString(error_type, message);
        return -1;
    }
    PyObject *is_aw = PyObject_CallOneArg(st->is_awaitable_fn, result);
    if (is_aw == NULL) {
        return -1;
    }
    int aw = PyObject_IsTrue(is_aw);
    Py_DECREF(is_aw);
    if (aw < 0) {
        return -1;
    }
    if (aw) {
        PyErr_SetString(error_type, message);
        return -1;
    }
    return 0;
}

static void ef_raise_exception_group(CoreState *st, const char *msg, PyObject *errors_tuple) {
    PyObject *msg_obj = PyUnicode_FromString(msg);
    if (msg_obj == NULL) {
        return;
    }
    PyObject *args[2] = {msg_obj, errors_tuple};
    PyObject *exc = PyObject_Vectorcall(st->exception_group_type, args, 2, NULL);
    Py_DECREF(msg_obj);
    if (exc != NULL) {
        PyErr_SetObject(st->exception_group_type, exc);
        Py_DECREF(exc);
    }
}

/* ================================ Flag ================================ */

typedef struct {
    PyObject_HEAD
    uint8_t value;
} FlagObject;

static int Flag_init(FlagObject *self, PyObject *args, PyObject *kwds) {
    static char *kwlist[] = {"value", NULL};
    int val = 1;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|p:Flag", kwlist, &val)) {
        return -1;
    }
    ef_atomic_store_u8(&self->value, val ? 1 : 0);
    return 0;
}

static PyObject *Flag_get(FlagObject *self, PyObject *Py_UNUSED(ignored)) {
    return PyBool_FromLong(ef_atomic_load_u8(&self->value));
}

static PyObject *Flag_set(FlagObject *self, PyObject *arg) {
    int val = PyObject_IsTrue(arg);
    if (val < 0) {
        return NULL;
    }
    ef_atomic_store_u8(&self->value, val ? 1 : 0);
    Py_RETURN_NONE;
}

static PyMethodDef Flag_methods[] = {
    {"get", (PyCFunction)Flag_get, METH_NOARGS, NULL},
    {"set", (PyCFunction)Flag_set, METH_O, NULL},
    {NULL, NULL, 0, NULL},
};

static PyType_Slot Flag_slots[] = {
    {Py_tp_doc, "Atomic boolean flag for fast active checks."},
    {Py_tp_init, (void *)Flag_init},
    {Py_tp_methods, Flag_methods},
    {0, NULL},
};

static PyType_Spec Flag_spec = {
    .name = "eventflow._core.Flag",
    .basicsize = sizeof(FlagObject),
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = Flag_slots,
};

/* ============================= Subscriber ============================= */

typedef struct {
    PyObject_HEAD
    PyObject *callback;
    PyObject *event_filter;
    PyObject *subscription;
    FlagObject *active_flag;
    PyObject *event_type;
    PyObject *predicate;
    PyObject *filter_matches;
    PyObject *remaining_obj;
    PyObject *pending;
    PyObject *py_lock;
    CoreState *state;
    PyThread_type_lock c_lock;
    Py_ssize_t remaining; /* -1 = unlimited (None), -2 = bigint > PY_SSIZE_T_MAX */
    uint8_t match_all;
    uint8_t exact_filter;
    uint8_t replaying;
} SubscriberObject;

static inline int sub_ensure_clock(SubscriberObject *self) {
    if (self->c_lock != NULL) {
        return 0;
    }
    PyThread_type_lock lock = PyThread_allocate_lock();
    if (lock == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    self->c_lock = lock;
    return 0;
}

static inline void sub_lock_acquire(SubscriberObject *self) {
    if (!PyThread_acquire_lock(self->c_lock, NOWAIT_LOCK)) {
        Py_BEGIN_ALLOW_THREADS
        PyThread_acquire_lock(self->c_lock, WAIT_LOCK);
        Py_END_ALLOW_THREADS
    }
}

static inline void sub_lock_release(SubscriberObject *self) {
    PyThread_release_lock(self->c_lock);
}

static int Subscriber_traverse(SubscriberObject *self, visitproc visit, void *arg) {
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->callback);
    Py_VISIT(self->event_filter);
    Py_VISIT(self->subscription);
    Py_VISIT((PyObject *)self->active_flag);
    Py_VISIT(self->event_type);
    Py_VISIT(self->predicate);
    Py_VISIT(self->filter_matches);
    Py_VISIT(self->remaining_obj);
    Py_VISIT(self->pending);
    Py_VISIT(self->py_lock);
    return 0;
}

static int Subscriber_clear(SubscriberObject *self) {
    Py_CLEAR(self->callback);
    Py_CLEAR(self->event_filter);
    Py_CLEAR(self->subscription);
    Py_CLEAR(self->active_flag);
    Py_CLEAR(self->event_type);
    Py_CLEAR(self->predicate);
    Py_CLEAR(self->filter_matches);
    Py_CLEAR(self->remaining_obj);
    Py_CLEAR(self->pending);
    Py_CLEAR(self->py_lock);
    return 0;
}

static void Subscriber_dealloc(SubscriberObject *self) {
    PyObject_GC_UnTrack(self);
    if (self->c_lock != NULL) {
        PyThread_free_lock(self->c_lock);
        self->c_lock = NULL;
    }
    Subscriber_clear(self);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Subscriber_init(SubscriberObject *self, PyObject *args, PyObject *kwds) {
    static char *kwlist[] = {
        "callback", "event_filter", "remaining", "subscription",
        "match_all", "replaying", "pending", NULL
    };
    PyObject *callback, *event_filter, *remaining_obj, *subscription;
    PyObject *pending = Py_None;
    int match_all = 0;
    int replaying = 0;
    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "OOOO|ppO:Subscriber", kwlist,
            &callback, &event_filter, &remaining_obj, &subscription,
            &match_all, &replaying, &pending)) {
        return -1;
    }
    PyObject *mod = PyType_GetModule(Py_TYPE(self));
    CoreState *st = get_core_state(mod);
    self->state = st;

    self->callback = Py_NewRef(callback);
    self->event_filter = Py_NewRef(event_filter);
    self->subscription = Py_NewRef(subscription);
    self->remaining_obj = Py_NewRef(remaining_obj);
    self->pending = Py_NewRef(pending);
    self->match_all = match_all ? 1 : 0;
    ef_atomic_store_u8(&self->replaying, replaying ? 1 : 0);

    PyObject *flag_obj = PyObject_GetAttrString(subscription, "_active_flag");
    if (flag_obj == NULL) {
        PyErr_Clear();
        self->active_flag = NULL;
    } else if (Py_IS_TYPE(flag_obj, st->flag_type)) {
        self->active_flag = (FlagObject *)flag_obj;
    } else {
        Py_DECREF(flag_obj);
        self->active_flag = NULL;
    }

    self->exact_filter = ((PyObject *)Py_TYPE(event_filter) == st->event_filter_type) ? 1 : 0;
    if (self->exact_filter) {
        PyObject *et = PyObject_GetAttrString(event_filter, "event_type");
        if (et == NULL) {
            return -1;
        }
        self->event_type = (et == Py_None) ? (Py_DECREF(et), NULL) : et;
        PyObject *pred = PyObject_GetAttrString(event_filter, "predicate");
        if (pred == NULL) {
            return -1;
        }
        self->predicate = (pred == Py_None) ? (Py_DECREF(pred), NULL) : pred;
        self->filter_matches = NULL;
    } else {
        self->event_type = NULL;
        self->predicate = NULL;
        self->filter_matches = PyObject_GetAttr(event_filter, st->k_matches);
        if (self->filter_matches == NULL) {
            return -1;
        }
    }

    if (remaining_obj == Py_None) {
        self->remaining = -1;
    } else {
        Py_ssize_t val = PyLong_AsSsize_t(remaining_obj);
        if (val == -1 && PyErr_Occurred()) {
            PyErr_Clear();
            self->remaining = -2;
        } else {
            self->remaining = val;
        }
    }
    if (self->remaining != -1 || replaying) {
        if (sub_ensure_clock(self) < 0) {
            return -1;
        }
    }
    return 0;
}

static inline int sub_is_active(CoreState *st, SubscriberObject *self) {
    if (self->active_flag != NULL) {
        return ef_atomic_load_u8(&self->active_flag->value) != 0;
    }
    PyObject *val = PyObject_GetAttr(self->subscription, st->k__active);
    if (val == NULL) {
        return -1;
    }
    int active = PyObject_IsTrue(val);
    Py_DECREF(val);
    return active;
}

static int sub_claim_internal(SubscriberObject *self, int *claimed, int *is_final) {
    CoreState *st = self->state;
    if (self->remaining == -1) {
        int active = sub_is_active(st, self);
        if (active < 0) {
            return -1;
        }
        *claimed = active ? 1 : 0;
        *is_final = 0;
        return 0;
    }
    if (sub_ensure_clock(self) < 0) {
        return -1;
    }
    sub_lock_acquire(self);
    int active = sub_is_active(st, self);
    if (active < 0) {
        sub_lock_release(self);
        return -1;
    }
    if (!active) {
        sub_lock_release(self);
        *claimed = 0;
        *is_final = 0;
        return 0;
    }
    if (self->remaining == -2) {
        PyObject *one = PyLong_FromLong(1);
        if (one == NULL) {
            sub_lock_release(self);
            return -1;
        }
        int le_zero = PyObject_RichCompareBool(self->remaining_obj, one, Py_LT);
        if (le_zero < 0 || le_zero) {
            Py_DECREF(one);
            sub_lock_release(self);
            if (le_zero < 0) {
                return -1;
            }
            *claimed = 0;
            *is_final = 0;
            return 0;
        }
        PyObject *next_rem = PyNumber_Subtract(self->remaining_obj, one);
        Py_DECREF(one);
        if (next_rem == NULL) {
            sub_lock_release(self);
            return -1;
        }
        Py_SETREF(self->remaining_obj, next_rem);
        Py_ssize_t narrow = PyLong_AsSsize_t(next_rem);
        if (narrow == -1 && PyErr_Occurred()) {
            PyErr_Clear();
            *is_final = 0;
        } else {
            self->remaining = narrow;
            *is_final = (narrow == 0) ? 1 : 0;
        }
        sub_lock_release(self);
        *claimed = 1;
        return 0;
    }
    if (self->remaining <= 0) {
        sub_lock_release(self);
        *claimed = 0;
        *is_final = 0;
        return 0;
    }
    self->remaining -= 1;
    Py_ssize_t rem = self->remaining;
    PyObject *new_obj = PyLong_FromSsize_t(rem);
    if (new_obj != NULL) {
        Py_SETREF(self->remaining_obj, new_obj);
    }
    sub_lock_release(self);
    *claimed = 1;
    *is_final = (rem == 0) ? 1 : 0;
    return 0;
}

static PyObject *Subscriber_claim(SubscriberObject *self, PyObject *Py_UNUSED(ignored)) {
    int claimed = 0, is_final = 0;
    if (sub_claim_internal(self, &claimed, &is_final) < 0) {
        return NULL;
    }
    PyObject *res = PyTuple_New(2);
    if (res == NULL) {
        return NULL;
    }
    PyTuple_SET_ITEM(res, 0, PyBool_FromLong(claimed));
    PyTuple_SET_ITEM(res, 1, PyBool_FromLong(is_final));
    return res;
}

static int sub_restore_claim_internal(SubscriberObject *self) {
    if (self->remaining == -1) {
        return 0;
    }
    if (sub_ensure_clock(self) < 0) {
        return -1;
    }
    sub_lock_acquire(self);
    if (self->remaining == -2 || self->remaining == PY_SSIZE_T_MAX) {
        PyObject *one = PyLong_FromLong(1);
        if (one == NULL) {
            sub_lock_release(self);
            return -1;
        }
        PyObject *next_rem = PyNumber_Add(self->remaining_obj, one);
        Py_DECREF(one);
        if (next_rem == NULL) {
            sub_lock_release(self);
            return -1;
        }
        Py_SETREF(self->remaining_obj, next_rem);
        self->remaining = -2;
    } else {
        self->remaining += 1;
        PyObject *new_obj = PyLong_FromSsize_t(self->remaining);
        if (new_obj != NULL) {
            Py_SETREF(self->remaining_obj, new_obj);
        }
    }
    sub_lock_release(self);
    return 0;
}

static PyObject *Subscriber_restore_claim(SubscriberObject *self, PyObject *Py_UNUSED(ignored)) {
    if (sub_restore_claim_internal(self) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static int sub_buffer_if_replaying_internal(SubscriberObject *self, PyObject *event) {
    if (!ef_atomic_load_u8(&self->replaying)) {
        return 0;
    }
    if (sub_ensure_clock(self) < 0) {
        return -1;
    }
    sub_lock_acquire(self);
    if (!ef_atomic_load_u8(&self->replaying)) {
        sub_lock_release(self);
        return 0;
    }
    PyObject *pending = self->pending;
    PyObject *maxlen_obj = PyObject_GetAttrString(pending, "maxlen");
    if (maxlen_obj == NULL) {
        sub_lock_release(self);
        return -1;
    }
    if (maxlen_obj != Py_None) {
        Py_ssize_t maxlen = PyLong_AsSsize_t(maxlen_obj);
        Py_DECREF(maxlen_obj);
        if (maxlen == -1 && PyErr_Occurred()) {
            sub_lock_release(self);
            return -1;
        }
        Py_ssize_t cur_len = PyObject_Length(pending);
        if (cur_len < 0) {
            sub_lock_release(self);
            return -1;
        }
        if (cur_len >= maxlen) {
            sub_lock_release(self);
            PyErr_SetString(PyExc_BufferError, "Event replay buffer is full");
            return -1;
        }
    } else {
        Py_DECREF(maxlen_obj);
    }
    PyObject *app = PyObject_CallMethodOneArg(pending, self->state->k_append, event);
    sub_lock_release(self);
    if (app == NULL) {
        return -1;
    }
    Py_DECREF(app);
    return 1;
}

static PyObject *Subscriber_buffer_if_replaying(SubscriberObject *self, PyObject *event) {
    int rc = sub_buffer_if_replaying_internal(self, event);
    if (rc < 0) {
        return NULL;
    }
    return PyBool_FromLong(rc);
}

static PyObject *Subscriber_finish_replay(SubscriberObject *self, PyObject *Py_UNUSED(ignored)) {
    if (sub_ensure_clock(self) < 0) {
        return NULL;
    }
    sub_lock_acquire(self);
    PyObject *pending = self->pending;
    if (pending != Py_None) {
        Py_ssize_t len = PyObject_Length(pending);
        if (len < 0) {
            sub_lock_release(self);
            return NULL;
        }
        if (len > 0) {
            PyObject *events = PySequence_Tuple(pending);
            if (events == NULL) {
                sub_lock_release(self);
                return NULL;
            }
            PyObject *cleared = PyObject_CallMethodNoArgs(pending, self->state->k_clear);
            if (cleared == NULL) {
                Py_DECREF(events);
                sub_lock_release(self);
                return NULL;
            }
            Py_DECREF(cleared);
            sub_lock_release(self);
            return events;
        }
    }
    ef_atomic_store_u8(&self->replaying, 0);
    Py_SETREF(self->pending, Py_NewRef(Py_None));
    sub_lock_release(self);
    Py_RETURN_NONE;
}

static PyObject *Subscriber_get_replaying(SubscriberObject *self, void *Py_UNUSED(closure)) {
    return PyBool_FromLong(ef_atomic_load_u8(&self->replaying));
}

static int Subscriber_set_replaying(SubscriberObject *self, PyObject *value, void *Py_UNUSED(closure)) {
    int flag = PyObject_IsTrue(value);
    if (flag < 0) {
        return -1;
    }
    ef_atomic_store_u8(&self->replaying, flag ? 1 : 0);
    return 0;
}

static PyObject *Subscriber_get_lock(SubscriberObject *self, void *Py_UNUSED(closure)) {
    if (self->py_lock == NULL) {
        PyObject *lk = PyObject_CallNoArgs(self->state->threading_lock_fn);
        if (lk == NULL) {
            return NULL;
        }
        self->py_lock = lk;
    }
    return Py_NewRef(self->py_lock);
}

static PyObject *Subscriber_get_match_all(SubscriberObject *self, void *Py_UNUSED(closure)) {
    return PyBool_FromLong(self->match_all);
}

static PyMemberDef Subscriber_members[] = {
    {"callback", T_OBJECT_EX, offsetof(SubscriberObject, callback), READONLY, NULL},
    {"event_filter", T_OBJECT_EX, offsetof(SubscriberObject, event_filter), READONLY, NULL},
    {"remaining", T_OBJECT_EX, offsetof(SubscriberObject, remaining_obj), 0, NULL},
    {"subscription", T_OBJECT_EX, offsetof(SubscriberObject, subscription), READONLY, NULL},
    {"pending", T_OBJECT_EX, offsetof(SubscriberObject, pending), 0, NULL},
    {NULL, 0, 0, 0, NULL},
};

static PyGetSetDef Subscriber_getset[] = {
    {"replaying", (getter)Subscriber_get_replaying, (setter)Subscriber_set_replaying, NULL, NULL},
    {"lock", (getter)Subscriber_get_lock, NULL, NULL, NULL},
    {"match_all", (getter)Subscriber_get_match_all, NULL, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

static PyMethodDef Subscriber_methods[] = {
    {"claim", (PyCFunction)Subscriber_claim, METH_NOARGS, NULL},
    {"restore_claim", (PyCFunction)Subscriber_restore_claim, METH_NOARGS, NULL},
    {"buffer_if_replaying", (PyCFunction)Subscriber_buffer_if_replaying, METH_O, NULL},
    {"finish_replay", (PyCFunction)Subscriber_finish_replay, METH_NOARGS, NULL},
    {NULL, NULL, 0, NULL},
};

static PyType_Slot Subscriber_slots[] = {
    {Py_tp_doc, "Native EventBus subscriber record."},
    {Py_tp_init, (void *)Subscriber_init},
    {Py_tp_dealloc, (void *)Subscriber_dealloc},
    {Py_tp_traverse, (void *)Subscriber_traverse},
    {Py_tp_clear, (void *)Subscriber_clear},
    {Py_tp_methods, Subscriber_methods},
    {Py_tp_members, Subscriber_members},
    {Py_tp_getset, Subscriber_getset},
    {0, NULL},
};

static PyType_Spec Subscriber_spec = {
    .name = "eventflow._core.Subscriber",
    .basicsize = sizeof(SubscriberObject),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = Subscriber_slots,
};

/* =============================== BusCore ============================== */

typedef struct {
    PyObject_HEAD
    CoreState *state;
    PyObject *lock;
    PyObject *lock_acquire;
    PyObject *lock_release;
    PyObject *condition;
    PyObject *subscribers_dict;
    PyObject *unkeyed_dict;
    PyObject *keyed_dict;
    PyObject *route_snapshots;
    PyObject *all_snapshot;
    PyObject *unkeyed_snapshot;
    PyObject *history_append;
    PyObject *routing_key;
    PyObject *executor;
    PyObject *on_before;
    PyObject *on_after;
    PyObject *on_error;
    int64_t published;
    int64_t delivered;
    int64_t failed;
    int64_t waiter_count;
    uint8_t closed;
    uint8_t has_subscribers;
    uint8_t has_keyed;
    uint8_t error_raise;
    uint8_t fast_snapshot;
} BusCoreObject;

static int BusCore_traverse(BusCoreObject *self, visitproc visit, void *arg) {
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->lock);
    Py_VISIT(self->lock_acquire);
    Py_VISIT(self->lock_release);
    Py_VISIT(self->condition);
    Py_VISIT(self->subscribers_dict);
    Py_VISIT(self->unkeyed_dict);
    Py_VISIT(self->keyed_dict);
    Py_VISIT(self->route_snapshots);
    Py_VISIT(self->all_snapshot);
    Py_VISIT(self->unkeyed_snapshot);
    Py_VISIT(self->history_append);
    Py_VISIT(self->routing_key);
    Py_VISIT(self->executor);
    Py_VISIT(self->on_before);
    Py_VISIT(self->on_after);
    Py_VISIT(self->on_error);
    return 0;
}

static int BusCore_clear(BusCoreObject *self) {
    Py_CLEAR(self->lock);
    Py_CLEAR(self->lock_acquire);
    Py_CLEAR(self->lock_release);
    Py_CLEAR(self->condition);
    Py_CLEAR(self->subscribers_dict);
    Py_CLEAR(self->unkeyed_dict);
    Py_CLEAR(self->keyed_dict);
    Py_CLEAR(self->route_snapshots);
    Py_CLEAR(self->all_snapshot);
    Py_CLEAR(self->unkeyed_snapshot);
    Py_CLEAR(self->history_append);
    Py_CLEAR(self->routing_key);
    Py_CLEAR(self->executor);
    Py_CLEAR(self->on_before);
    Py_CLEAR(self->on_after);
    Py_CLEAR(self->on_error);
    return 0;
}

static void BusCore_dealloc(BusCoreObject *self) {
    PyObject_GC_UnTrack(self);
    BusCore_clear(self);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int BusCore_init(BusCoreObject *self, PyObject *args, PyObject *kwds) {
    static char *kwlist[] = {
        "lock", "condition", "subscribers", "unkeyed", "keyed",
        "route_snapshots", "history", "routing_key", "executor",
        "on_before", "on_after", "on_error", "error_raise", "fast_snapshot", NULL
    };
    PyObject *lock, *condition, *subscribers, *unkeyed, *keyed, *route_snapshots;
    PyObject *history, *routing_key, *executor, *on_before, *on_after, *on_error;
    int error_raise = 0, fast_snapshot = 1;
    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "OOOOOOOOOOOOpp:BusCore", kwlist,
            &lock, &condition, &subscribers, &unkeyed, &keyed,
            &route_snapshots, &history, &routing_key, &executor,
            &on_before, &on_after, &on_error, &error_raise, &fast_snapshot)) {
        return -1;
    }
    self->state = get_core_state(PyType_GetModule(Py_TYPE(self)));
    CoreState *st = self->state;

    self->lock = Py_NewRef(lock);
    self->lock_acquire = PyObject_GetAttr(lock, st->k_acquire);
    if (self->lock_acquire == NULL) return -1;
    self->lock_release = PyObject_GetAttr(lock, st->k_release);
    if (self->lock_release == NULL) return -1;
    self->condition = Py_NewRef(condition);
    self->subscribers_dict = Py_NewRef(subscribers);
    self->unkeyed_dict = Py_NewRef(unkeyed);
    self->keyed_dict = Py_NewRef(keyed);
    self->route_snapshots = Py_NewRef(route_snapshots);
    self->all_snapshot = NULL;
    self->unkeyed_snapshot = NULL;

    if (history == Py_None) {
        self->history_append = NULL;
    } else {
        self->history_append = PyObject_GetAttr(history, st->k_append);
        if (self->history_append == NULL) return -1;
    }
    self->routing_key = (routing_key == Py_None) ? NULL : Py_NewRef(routing_key);
    self->executor = (executor == Py_None) ? NULL : Py_NewRef(executor);
    self->on_before = (on_before == Py_None) ? NULL : Py_NewRef(on_before);
    self->on_after = (on_after == Py_None) ? NULL : Py_NewRef(on_after);
    self->on_error = (on_error == Py_None) ? NULL : Py_NewRef(on_error);

    self->published = 0;
    self->delivered = 0;
    self->failed = 0;
    self->waiter_count = 0;
    self->closed = 0;
    self->has_subscribers = 0;
    self->has_keyed = 0;
    self->error_raise = error_raise ? 1 : 0;
    self->fast_snapshot = fast_snapshot ? 1 : 0;
    return 0;
}

static inline void release_lock_preserving_error(BusCoreObject *core) {
#if PY_VERSION_HEX >= 0x030C0000
    PyObject *exc = PyErr_GetRaisedException();
    PyObject *rel = PyObject_Vectorcall(core->lock_release, NULL, 0, NULL);
    Py_XDECREF(rel);
    if (PyErr_Occurred()) {
        PyErr_Clear();
    }
    PyErr_SetRaisedException(exc);
#else
    PyObject *ptype, *pvalue, *ptraceback;
    PyErr_Fetch(&ptype, &pvalue, &ptraceback);
    PyObject *rel = PyObject_Vectorcall(core->lock_release, NULL, 0, NULL);
    Py_XDECREF(rel);
    if (PyErr_Occurred()) {
        PyErr_Clear();
    }
    PyErr_Restore(ptype, pvalue, ptraceback);
#endif
}

static PyObject *tuple_from_dict_values(PyObject *dict) {
    Py_ssize_t size = PyDict_GET_SIZE(dict);
    PyObject *tuple = PyTuple_New(size);
    if (tuple == NULL) {
        return NULL;
    }
    Py_ssize_t pos = 0, idx = 0;
    PyObject *key, *value;
    while (PyDict_Next(dict, &pos, &key, &value)) {
        if (idx >= size) {
            Py_DECREF(tuple);
            PyErr_SetString(PyExc_RuntimeError, "subscriber dict grew during snapshot");
            return NULL;
        }
        PyTuple_SET_ITEM(tuple, idx++, Py_NewRef(value));
    }
    if (idx != size) {
        Py_DECREF(tuple);
        PyErr_SetString(PyExc_RuntimeError, "subscriber dict shrank during snapshot");
        return NULL;
    }
    return tuple;
}

static PyObject *merge_subscriber_dicts_c(PyObject *unkeyed, PyObject *keyed) {
    Py_ssize_t left_count = PyDict_GET_SIZE(unkeyed);
    Py_ssize_t right_count = PyDict_GET_SIZE(keyed);
    PyObject *result = PyTuple_New(left_count + right_count);
    if (result == NULL) {
        return NULL;
    }
    Py_ssize_t left_pos = 0, right_pos = 0, out_idx = 0;
    PyObject *left_key = NULL, *left_val = NULL;
    PyObject *right_key = NULL, *right_val = NULL;
    int has_left = PyDict_Next(unkeyed, &left_pos, &left_key, &left_val);
    int has_right = PyDict_Next(keyed, &right_pos, &right_key, &right_val);
    while (has_left && has_right) {
        int left_le;
        int lo = 0, ro = 0;
        long long lv = PyLong_AsLongLongAndOverflow(left_key, &lo);
        long long rv = PyLong_AsLongLongAndOverflow(right_key, &ro);
        if (lo == 0 && ro == 0 && !(lv == -1 && PyErr_Occurred()) && !(rv == -1 && PyErr_Occurred())) {
            left_le = (lv <= rv);
        } else {
            if (PyErr_Occurred()) PyErr_Clear();
            left_le = PyObject_RichCompareBool(left_key, right_key, Py_LE);
            if (left_le < 0) {
                Py_DECREF(result);
                return NULL;
            }
        }
        if (left_le) {
            PyTuple_SET_ITEM(result, out_idx++, Py_NewRef(left_val));
            has_left = PyDict_Next(unkeyed, &left_pos, &left_key, &left_val);
        } else {
            PyTuple_SET_ITEM(result, out_idx++, Py_NewRef(right_val));
            has_right = PyDict_Next(keyed, &right_pos, &right_key, &right_val);
        }
    }
    while (has_left) {
        PyTuple_SET_ITEM(result, out_idx++, Py_NewRef(left_val));
        has_left = PyDict_Next(unkeyed, &left_pos, &left_key, &left_val);
    }
    while (has_right) {
        PyTuple_SET_ITEM(result, out_idx++, Py_NewRef(right_val));
        has_right = PyDict_Next(keyed, &right_pos, &right_key, &right_val);
    }
    if (out_idx != left_count + right_count) {
        Py_DECREF(result);
        PyErr_SetString(PyExc_RuntimeError, "subscriber maps changed during merge");
        return NULL;
    }
    return result;
}

static inline PyObject *make_publish_result(
    CoreState *st,
    Py_ssize_t matched,
    Py_ssize_t delivered,
    Py_ssize_t failed,
    PyObject *errors_tuple
) {
    if (matched == 0 && delivered == 0 && failed == 0 && PyTuple_GET_SIZE(errors_tuple) == 0) {
        return Py_NewRef(st->empty_publish_result);
    }
    PyTypeObject *pr_tp = (PyTypeObject *)st->publish_result_type;
    PyObject *res = pr_tp->tp_new(pr_tp, st->empty_tuple, NULL);
    if (res == NULL) {
        return NULL;
    }
    PyObject *m_obj = PyLong_FromSsize_t(matched);
    PyObject *d_obj = PyLong_FromSsize_t(delivered);
    PyObject *f_obj = PyLong_FromSsize_t(failed);
    if (m_obj == NULL || d_obj == NULL || f_obj == NULL) {
        Py_XDECREF(m_obj);
        Py_XDECREF(d_obj);
        Py_XDECREF(f_obj);
        Py_DECREF(res);
        return NULL;
    }
    if (
        Py_TYPE(st->descr_matched)->tp_descr_set(st->descr_matched, res, m_obj) < 0
        || Py_TYPE(st->descr_delivered)->tp_descr_set(st->descr_delivered, res, d_obj) < 0
        || Py_TYPE(st->descr_failed)->tp_descr_set(st->descr_failed, res, f_obj) < 0
        || Py_TYPE(st->descr_errors)->tp_descr_set(st->descr_errors, res, errors_tuple) < 0
    ) {
        Py_DECREF(m_obj);
        Py_DECREF(d_obj);
        Py_DECREF(f_obj);
        Py_DECREF(res);
        return NULL;
    }
    Py_DECREF(m_obj);
    Py_DECREF(d_obj);
    Py_DECREF(f_obj);
    return res;
}

static int append_current_exception(PyObject **errors_list) {
    if (!PyErr_ExceptionMatches(PyExc_Exception)) {
        return -1; /* BaseException propagates untouched */
    }
    if (*errors_list == NULL) {
        *errors_list = PyList_New(0);
        if (*errors_list == NULL) {
            return -1;
        }
    }
#if PY_VERSION_HEX >= 0x030C0000
    PyObject *exc = PyErr_GetRaisedException();
    int rc = PyList_Append(*errors_list, exc);
    Py_DECREF(exc);
    return rc;
#else
    PyObject *ptype, *pvalue, *ptraceback;
    PyErr_Fetch(&ptype, &pvalue, &ptraceback);
    PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
    if (ptraceback != NULL) {
        PyException_SetTraceback(pvalue, ptraceback);
    }
    int rc = PyList_Append(*errors_list, pvalue);
    Py_XDECREF(ptype);
    Py_XDECREF(pvalue);
    Py_XDECREF(ptraceback);
    return rc;
#endif
}

static int notify_errors_c(
    BusCoreObject *core,
    PyObject *event,
    PyObject *slice_source,
    Py_ssize_t start_idx,
    Py_ssize_t end_idx,
    PyObject *dest_errors
) {
    CoreState *st = core->state;
    PyObject *action = core->on_error;
    for (Py_ssize_t i = start_idx; i < end_idx; ++i) {
        PyObject *err = PyList_GET_ITEM(slice_source, i);
        PyObject *ctx_args[2] = {event, err};
        PyObject *ctx = PyObject_Vectorcall(st->error_context_type, ctx_args, 2, NULL);
        if (ctx == NULL) {
            return -1;
        }
        PyObject *res = PyObject_CallOneArg(action, ctx);
        Py_DECREF(ctx);
        if (res == NULL) {
            if (append_current_exception(&dest_errors) < 0) {
                return -1;
            }
            continue;
        }
        int aw_rc = ef_check_not_awaitable(
            st, res, PyExc_TypeError, "Synchronous callback returned an awaitable"
        );
        Py_DECREF(res);
        if (aw_rc < 0) {
            if (append_current_exception(&dest_errors) < 0) {
                return -1;
            }
        }
    }
    return 0;
}

static PyObject *BusCore_invalidate_snapshots(BusCoreObject *self, PyObject *args) {
    int has_subs = 0, has_keyed = 0;
    if (!PyArg_ParseTuple(args, "pp:invalidate_snapshots", &has_subs, &has_keyed)) {
        return NULL;
    }
    Py_CLEAR(self->all_snapshot);
    Py_CLEAR(self->unkeyed_snapshot);
    self->has_subscribers = has_subs ? 1 : 0;
    self->has_keyed = has_keyed ? 1 : 0;
    Py_RETURN_NONE;
}

static PyObject *BusCore_set_closed(BusCoreObject *self, PyObject *arg) {
    int closed = PyObject_IsTrue(arg);
    if (closed < 0) return NULL;
    ef_atomic_store_u8(&self->closed, closed ? 1 : 0);
    Py_RETURN_NONE;
}

static PyObject *BusCore_add_stats(BusCoreObject *self, PyObject *args) {
    long long pub = 0, deliv = 0, fail = 0;
    if (!PyArg_ParseTuple(args, "LLL:add_stats", &pub, &deliv, &fail)) {
        return NULL;
    }
    if (pub) self->published += pub;
    if (deliv) ef_atomic_add_i64(&self->delivered, deliv);
    if (fail) ef_atomic_add_i64(&self->failed, fail);
    Py_RETURN_NONE;
}

static PyObject *BusCore_inc_waiters(BusCoreObject *self, PyObject *Py_UNUSED(ignored)) {
    ef_atomic_add_i64(&self->waiter_count, 1);
    Py_RETURN_NONE;
}

static PyObject *BusCore_dec_waiters(BusCoreObject *self, PyObject *Py_UNUSED(ignored)) {
    ef_atomic_add_i64(&self->waiter_count, -1);
    Py_RETURN_NONE;
}

static PyObject *BusCore_stats(BusCoreObject *self, PyObject *Py_UNUSED(ignored)) {
    CoreState *st = self->state;
    PyObject *p = PyLong_FromLongLong(self->published);
    PyObject *d = PyLong_FromLongLong(ef_atomic_load_i64(&self->delivered));
    PyObject *f = PyLong_FromLongLong(ef_atomic_load_i64(&self->failed));
    if (p == NULL || d == NULL || f == NULL) {
        Py_XDECREF(p);
        Py_XDECREF(d);
        Py_XDECREF(f);
        return NULL;
    }
    PyObject *args[3] = {p, d, f};
    PyObject *res = PyObject_Vectorcall(st->event_bus_stats_type, args, 3, NULL);
    Py_DECREF(p);
    Py_DECREF(d);
    Py_DECREF(f);
    return res;
}

static PyObject *BusCore_publish(BusCoreObject *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 2) {
        PyErr_SetString(PyExc_TypeError, "publish() expects (bus, event)");
        return NULL;
    }
    PyObject *bus = args[0];
    PyObject *event = args[1];
    CoreState *st = self->state;

    /* Fall back cleanly if executor is set or tests/benchmarks swapped bus._lock. */
    if (self->executor != NULL) {
        return PyObject_CallMethodOneArg(bus, st->k__publish_python, event);
    }
    PyObject *cur_lock = PyObject_GetAttr(bus, st->k__lock);
    if (cur_lock == NULL) {
        return NULL;
    }
    int same_lock = (cur_lock == self->lock);
    Py_DECREF(cur_lock);
    if (!same_lock) {
        return PyObject_CallMethodOneArg(bus, st->k__publish_python, event);
    }

    PyObject *acq = PyObject_Vectorcall(self->lock_acquire, NULL, 0, NULL);
    if (acq == NULL) {
        return NULL;
    }
    Py_DECREF(acq);

    if (ef_atomic_load_u8(&self->closed)) {
        PyObject *rel = PyObject_Vectorcall(self->lock_release, NULL, 0, NULL);
        Py_XDECREF(rel);
        PyErr_SetString(st->closed_error_type, "Event bus is closed");
        return NULL;
    }

    if (!self->has_subscribers && self->on_before == NULL && self->on_after == NULL) {
        if (self->history_append != NULL) {
            PyObject *rec = PyObject_CallOneArg(self->history_append, event);
            if (rec == NULL) {
                release_lock_preserving_error(self);
                return NULL;
            }
            Py_DECREF(rec);
        }
        self->published += 1;
        PyObject *rel = PyObject_Vectorcall(self->lock_release, NULL, 0, NULL);
        if (rel == NULL) {
            return NULL;
        }
        Py_DECREF(rel);
        return Py_NewRef(st->empty_publish_result);
    }

    PyObject *event_key = NULL;
    if (self->has_keyed) {
        if (self->routing_key != NULL) {
            event_key = PyObject_CallOneArg(self->routing_key, event);
            if (event_key == NULL) {
                release_lock_preserving_error(self);
                return NULL;
            }
            if (event_key == Py_None) {
                Py_DECREF(event_key);
                release_lock_preserving_error(self);
                PyErr_SetString(PyExc_TypeError, "routing_key function must return a non-None hashable value");
                return NULL;
            }
            if (PyObject_Hash(event_key) == -1) {
                Py_DECREF(event_key);
                if (PyErr_ExceptionMatches(PyExc_TypeError)) {
                    PyErr_SetString(PyExc_TypeError, "routing_key function must return a hashable value");
                }
                release_lock_preserving_error(self);
                return NULL;
            }
        }
    }

    PyObject *snapshot = NULL;
    if (!self->fast_snapshot) {
        PyObject *arg_key = (event_key != NULL) ? event_key : Py_None;
        snapshot = PyObject_CallMethodOneArg(bus, st->k__subscriber_snapshot, arg_key);
    } else if (self->routing_key == NULL) {
        if (self->all_snapshot == NULL) {
            self->all_snapshot = tuple_from_dict_values(self->subscribers_dict);
        }
        if (self->all_snapshot != NULL) {
            snapshot = Py_NewRef(self->all_snapshot);
        }
    } else if (event_key == NULL) {
        if (self->unkeyed_snapshot == NULL) {
            self->unkeyed_snapshot = tuple_from_dict_values(self->unkeyed_dict);
        }
        if (self->unkeyed_snapshot != NULL) {
            snapshot = Py_NewRef(self->unkeyed_snapshot);
        }
    } else {
        PyObject *keyed = NULL;
        int krc = ef_dict_get_item_ref(self->keyed_dict, event_key, &keyed);
        if (krc < 0) {
            Py_DECREF(event_key);
            release_lock_preserving_error(self);
            return NULL;
        }
        if (keyed == NULL || PyDict_GET_SIZE(keyed) == 0) {
            Py_XDECREF(keyed);
            if (self->unkeyed_snapshot == NULL) {
                self->unkeyed_snapshot = tuple_from_dict_values(self->unkeyed_dict);
            }
            if (self->unkeyed_snapshot != NULL) {
                snapshot = Py_NewRef(self->unkeyed_snapshot);
            }
        } else {
            int rrc = ef_dict_get_item_ref(self->route_snapshots, event_key, &snapshot);
            if (rrc < 0) {
                Py_DECREF(keyed);
                Py_DECREF(event_key);
                release_lock_preserving_error(self);
                return NULL;
            }
            if (snapshot == NULL) {
                if (PyDict_GET_SIZE(self->unkeyed_dict) == 0) {
                    snapshot = tuple_from_dict_values(keyed);
                } else {
                    snapshot = merge_subscriber_dicts_c(self->unkeyed_dict, keyed);
                }
                if (snapshot != NULL) {
                    if (PyDict_SetItem(self->route_snapshots, event_key, snapshot) < 0) {
                        Py_CLEAR(snapshot);
                    }
                }
            }
            Py_DECREF(keyed);
        }
    }
    Py_XDECREF(event_key);
    if (snapshot == NULL) {
        release_lock_preserving_error(self);
        return NULL;
    }
    if (!PyTuple_CheckExact(snapshot)) {
        PyObject *as_tuple = PySequence_Tuple(snapshot);
        Py_DECREF(snapshot);
        if (as_tuple == NULL) {
            release_lock_preserving_error(self);
            return NULL;
        }
        snapshot = as_tuple;
    }

    if (self->history_append != NULL) {
        PyObject *rec = PyObject_CallOneArg(self->history_append, event);
        if (rec == NULL) {
            Py_DECREF(snapshot);
            release_lock_preserving_error(self);
            return NULL;
        }
        Py_DECREF(rec);
    }
    self->published += 1;

    PyObject *rel = PyObject_Vectorcall(self->lock_release, NULL, 0, NULL);
    if (rel == NULL) {
        Py_DECREF(snapshot);
        return NULL;
    }
    Py_DECREF(rel);

    Py_ssize_t total_subs = PyTuple_GET_SIZE(snapshot);
    SubscriberObject *stack_matched[64];
    SubscriberObject **matched = stack_matched;
    if (total_subs > 64) {
        matched = (SubscriberObject **)PyMem_Malloc((size_t)total_subs * sizeof(SubscriberObject *));
        if (matched == NULL) {
            Py_DECREF(snapshot);
            PyErr_NoMemory();
            return NULL;
        }
    }

    PyObject *errors = NULL;
    Py_ssize_t matched_count = 0;
    Py_ssize_t delivered = 0;

    if (self->on_before != NULL) {
        PyObject *bres = PyObject_CallOneArg(self->on_before, event);
        if (bres == NULL) {
            if (append_current_exception(&errors) < 0) {
                goto fail_cleanup;
            }
        } else {
            int aw_rc = ef_check_not_awaitable(
                st, bres, PyExc_TypeError, "Synchronous callback returned an awaitable"
            );
            Py_DECREF(bres);
            if (aw_rc < 0 && append_current_exception(&errors) < 0) {
                goto fail_cleanup;
            }
        }
    }

    for (Py_ssize_t i = 0; i < total_subs; ++i) {
        PyObject *item = PyTuple_GET_ITEM(snapshot, i);
        if (!Py_IS_TYPE(item, st->subscriber_type)) {
            Py_DECREF(snapshot);
            if (matched != stack_matched) PyMem_Free(matched);
            Py_XDECREF(errors);
            return PyObject_CallMethodOneArg(bus, st->k__publish_python, event);
        }
        SubscriberObject *sub = (SubscriberObject *)item;
        int active = sub_is_active(st, sub);
        if (active < 0) {
            if (append_current_exception(&errors) < 0) goto fail_cleanup;
            continue;
        }
        if (!active) {
            continue;
        }
        if (!sub->match_all) {
            if (sub->exact_filter) {
                if (sub->event_type != NULL) {
                    int m = PyObject_IsInstance(event, sub->event_type);
                    if (m < 0) {
                        if (append_current_exception(&errors) < 0) goto fail_cleanup;
                        continue;
                    }
                    if (!m) {
                        continue;
                    }
                }
                if (sub->predicate != NULL) {
                    PyObject *pres = PyObject_CallOneArg(sub->predicate, event);
                    if (pres == NULL) {
                        if (append_current_exception(&errors) < 0) goto fail_cleanup;
                        continue;
                    }
                    if (pres == Py_True) {
                        Py_DECREF(pres);
                    } else if (pres == Py_False) {
                        Py_DECREF(pres);
                        continue;
                    } else {
                        int aw_rc = ef_check_not_awaitable(
                            st,
                            pres,
                            PyExc_TypeError,
                            "Event filter predicates must return a boolean synchronously"
                        );
                        Py_DECREF(pres);
                        if (aw_rc == 0) {
                            PyErr_SetString(PyExc_TypeError, "Event filter predicates must return bool");
                        }
                        if (append_current_exception(&errors) < 0) goto fail_cleanup;
                        continue;
                    }
                }
            } else {
                PyObject *mres = PyObject_CallOneArg(sub->filter_matches, event);
                if (mres == NULL) {
                    if (append_current_exception(&errors) < 0) goto fail_cleanup;
                    continue;
                }
                int truth = PyObject_IsTrue(mres);
                Py_DECREF(mres);
                if (truth < 0) {
                    if (append_current_exception(&errors) < 0) goto fail_cleanup;
                    continue;
                }
                if (!truth) {
                    continue;
                }
            }
        }
        if (ef_atomic_load_u8(&sub->replaying)) {
            int brc = sub_buffer_if_replaying_internal(sub, event);
            if (brc < 0) {
                if (PyErr_ExceptionMatches(PyExc_BufferError)) {
                    PyObject *orig = NULL;
#if PY_VERSION_HEX >= 0x030C0000
                    orig = PyErr_GetRaisedException();
#else
                    PyObject *pt, *pv, *ptb;
                    PyErr_Fetch(&pt, &pv, &ptb);
                    PyErr_NormalizeException(&pt, &pv, &ptb);
                    if (ptb) PyException_SetTraceback(pv, ptb);
                    orig = pv;
                    Py_XDECREF(pt);
                    Py_XDECREF(ptb);
#endif
                    PyObject *cres = PyObject_CallMethodNoArgs(sub->subscription, st->k_cancel);
                    if (cres == NULL) {
                        Py_DECREF(orig);
                        goto fail_cleanup;
                    }
                    Py_DECREF(cres);
                    if (errors == NULL) {
                        errors = PyList_New(0);
                        if (errors == NULL) {
                            Py_DECREF(orig);
                            goto fail_cleanup;
                        }
                    }
                    int arc = PyList_Append(errors, orig);
                    Py_DECREF(orig);
                    if (arc < 0) goto fail_cleanup;
                    continue;
                }
                if (append_current_exception(&errors) < 0) goto fail_cleanup;
                continue;
            }
            if (brc > 0) {
                continue;
            }
        }
        matched[matched_count++] = sub;
    }

    for (Py_ssize_t j = 0; j < matched_count; ++j) {
        SubscriberObject *sub = matched[j];
        if (sub->remaining == -1) {
            int active = sub_is_active(st, sub);
            if (active < 0) {
                if (append_current_exception(&errors) < 0) goto fail_cleanup;
                continue;
            }
            if (!active) {
                continue;
            }
            PyObject *cres = PyObject_CallOneArg(sub->callback, event);
            if (cres == NULL) {
                if (append_current_exception(&errors) < 0) goto fail_cleanup;
                continue;
            }
            if (cres != Py_None && !ef_is_non_awaitable_type(cres)) {
                int aw_rc = ef_check_not_awaitable(
                    st,
                    cres,
                    st->invalid_handler_error_type,
                    "Synchronous handlers must not return awaitables; use AsyncEventBus"
                );
                Py_DECREF(cres);
                if (aw_rc < 0) {
                    if (append_current_exception(&errors) < 0) goto fail_cleanup;
                    continue;
                }
            } else {
                Py_DECREF(cres);
            }
            delivered += 1;
        } else {
            int claimed = 0, is_final = 0;
            if (sub_claim_internal(sub, &claimed, &is_final) < 0) {
                goto fail_cleanup;
            }
            if (!claimed) {
                continue;
            }
            if (is_final) {
                PyObject *cons = PyObject_CallMethodNoArgs(sub->subscription, st->k__consume);
                if (cons == NULL) {
#if PY_VERSION_HEX >= 0x030C0000
                    PyObject *exc = PyErr_GetRaisedException();
                    sub_restore_claim_internal(sub);
                    PyErr_SetRaisedException(exc);
#else
                    PyObject *pt, *pv, *ptb;
                    PyErr_Fetch(&pt, &pv, &ptb);
                    sub_restore_claim_internal(sub);
                    PyErr_Restore(pt, pv, ptb);
#endif
                    goto fail_cleanup;
                }
                Py_DECREF(cons);
            }
            PyObject *cres = PyObject_CallOneArg(sub->callback, event);
            if (cres == NULL) {
                if (append_current_exception(&errors) < 0) goto fail_cleanup;
                continue;
            }
            if (cres != Py_None && !ef_is_non_awaitable_type(cres)) {
                int aw_rc = ef_check_not_awaitable(
                    st,
                    cres,
                    st->invalid_handler_error_type,
                    "Synchronous handlers must not return awaitables; use AsyncEventBus"
                );
                Py_DECREF(cres);
                if (aw_rc < 0) {
                    if (append_current_exception(&errors) < 0) goto fail_cleanup;
                    continue;
                }
            } else {
                Py_DECREF(cres);
            }
            delivered += 1;
        }
    }

    if (matched != stack_matched) {
        PyMem_Free(matched);
        matched = stack_matched;
    }
    Py_CLEAR(snapshot);

    if (errors != NULL && PyList_GET_SIZE(errors) > 0 && self->on_error != NULL) {
        Py_ssize_t orig_len = PyList_GET_SIZE(errors);
        if (notify_errors_c(self, event, errors, 0, orig_len, errors) < 0) {
            Py_XDECREF(errors);
            return NULL;
        }
    }

    Py_ssize_t failed_count = (errors != NULL) ? PyList_GET_SIZE(errors) : 0;
    PyObject *errors_tuple = (errors != NULL) ? PyList_AsTuple(errors) : Py_NewRef(st->empty_tuple);
    if (errors_tuple == NULL) {
        Py_XDECREF(errors);
        return NULL;
    }
    PyObject *result = make_publish_result(st, matched_count, delivered, failed_count, errors_tuple);
    Py_DECREF(errors_tuple);
    if (result == NULL) {
        Py_XDECREF(errors);
        return NULL;
    }

    if (self->on_after != NULL) {
        PyObject *ares = PyObject_CallFunctionObjArgs(self->on_after, event, result, NULL);
        if (ares == NULL || ef_check_not_awaitable(
                st, ares, PyExc_TypeError, "Synchronous callback returned an awaitable"
            ) < 0) {
            Py_XDECREF(ares);
            if (append_current_exception(&errors) < 0) {
                Py_DECREF(result);
                Py_XDECREF(errors);
                return NULL;
            }
            if (self->on_error != NULL) {
                Py_ssize_t idx = PyList_GET_SIZE(errors) - 1;
                if (notify_errors_c(self, event, errors, idx, idx + 1, errors) < 0) {
                    Py_DECREF(result);
                    Py_DECREF(errors);
                    return NULL;
                }
            }
            failed_count = PyList_GET_SIZE(errors);
            PyObject *new_errs = PyList_AsTuple(errors);
            if (new_errs == NULL) {
                Py_DECREF(result);
                Py_DECREF(errors);
                return NULL;
            }
            PyObject *new_result = make_publish_result(st, matched_count, delivered, failed_count, new_errs);
            Py_DECREF(new_errs);
            Py_DECREF(result);
            if (new_result == NULL) {
                Py_DECREF(errors);
                return NULL;
            }
            result = new_result;
        } else {
            Py_DECREF(ares);
        }
    }

    if (delivered > 0 || failed_count > 0 || ef_atomic_load_i64(&self->waiter_count) > 0) {
        if (ef_atomic_load_i64(&self->waiter_count) == 0) {
            ef_atomic_add_i64(&self->delivered, delivered);
            ef_atomic_add_i64(&self->failed, failed_count);
        } else {
            PyObject *ent = PyObject_CallMethodNoArgs(self->condition, st->k___enter__);
            if (ent == NULL) {
                Py_DECREF(result);
                Py_XDECREF(errors);
                return NULL;
            }
            Py_DECREF(ent);
            ef_atomic_add_i64(&self->delivered, delivered);
            ef_atomic_add_i64(&self->failed, failed_count);
            PyObject *exc_res = NULL;
            if (ef_atomic_load_i64(&self->waiter_count) > 0) {
                PyObject *notified = PyObject_CallMethodNoArgs(self->condition, st->k_notify_all);
                if (notified == NULL) {
#if PY_VERSION_HEX >= 0x030C0000
                    exc_res = PyErr_GetRaisedException();
#else
                    PyObject *pt, *ptb;
                    PyErr_Fetch(&pt, &exc_res, &ptb);
                    Py_XDECREF(pt);
                    Py_XDECREF(ptb);
#endif
                } else {
                    Py_DECREF(notified);
                }
            }
            PyObject *ex_args[3] = {Py_None, Py_None, Py_None};
            PyObject *ex_m = PyObject_GetAttr(self->condition, st->k___exit__);
            if (ex_m != NULL) {
                PyObject *ex = PyObject_Vectorcall(ex_m, ex_args, 3, NULL);
                Py_DECREF(ex_m);
                Py_XDECREF(ex);
            }
            if (exc_res != NULL) {
#if PY_VERSION_HEX >= 0x030C0000
                PyErr_SetRaisedException(exc_res);
#else
                PyErr_Restore((PyObject *)Py_TYPE(exc_res), exc_res, NULL);
#endif
                Py_DECREF(result);
                Py_XDECREF(errors);
                return NULL;
            }
        }
    }

    if (failed_count > 0 && self->error_raise) {
        PyObject *final_errs = PyList_AsTuple(errors);
        Py_DECREF(errors);
        Py_DECREF(result);
        if (final_errs != NULL) {
            ef_raise_exception_group(st, "Event delivery failed", final_errs);
            Py_DECREF(final_errs);
        }
        return NULL;
    }

    Py_XDECREF(errors);
    return result;

fail_cleanup:
    if (matched != stack_matched) {
        PyMem_Free(matched);
    }
    Py_XDECREF(snapshot);
    Py_XDECREF(errors);
    return NULL;
}

static PyMethodDef BusCore_methods[] = {
    {"invalidate_snapshots", (PyCFunction)BusCore_invalidate_snapshots, METH_VARARGS, NULL},
    {"set_closed", (PyCFunction)BusCore_set_closed, METH_O, NULL},
    {"add_stats", (PyCFunction)BusCore_add_stats, METH_VARARGS, NULL},
    {"inc_waiters", (PyCFunction)BusCore_inc_waiters, METH_NOARGS, NULL},
    {"dec_waiters", (PyCFunction)BusCore_dec_waiters, METH_NOARGS, NULL},
    {"stats", (PyCFunction)BusCore_stats, METH_NOARGS, NULL},
    {"publish", (PyCFunction)(void (*)(void))BusCore_publish, METH_FASTCALL, NULL},
    {NULL, NULL, 0, NULL},
};

static PyType_Slot BusCore_slots[] = {
    {Py_tp_doc, "Native dispatch core for EventBus."},
    {Py_tp_init, (void *)BusCore_init},
    {Py_tp_dealloc, (void *)BusCore_dealloc},
    {Py_tp_traverse, (void *)BusCore_traverse},
    {Py_tp_clear, (void *)BusCore_clear},
    {Py_tp_methods, BusCore_methods},
    {0, NULL},
};

static PyType_Spec BusCore_spec = {
    .name = "eventflow._core.BusCore",
    .basicsize = sizeof(BusCoreObject),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = BusCore_slots,
};

/* ======================== OperationStatsCore ======================== */

typedef struct {
    PyObject *name;
    Py_hash_t hash;
    int64_t calls;
    int64_t successful;
    int64_t failed;
    int64_t cancelled;
    double total_seconds;
    double max_seconds;
    int64_t queue_depth;
    int64_t max_queue_depth;
    int64_t dropped;
    int64_t retries;
    int64_t rejected;
    int64_t timeouts;
    double shutdown_seconds;
} OpEntry;

typedef struct {
    PyObject_HEAD
    CoreState *state;
    PyThread_type_lock lock;
    Py_ssize_t max_operations;
    Py_ssize_t count;
    Py_ssize_t capacity;
    OpEntry *entries;
    Py_ssize_t *buckets;
    Py_ssize_t mask;
} OperationStatsCoreObject;

static inline void op_stats_lock_acquire(OperationStatsCoreObject *self) {
    if (!PyThread_acquire_lock(self->lock, NOWAIT_LOCK)) {
        Py_BEGIN_ALLOW_THREADS
        PyThread_acquire_lock(self->lock, WAIT_LOCK);
        Py_END_ALLOW_THREADS
    }
}

static inline void op_stats_lock_release(OperationStatsCoreObject *self) {
    PyThread_release_lock(self->lock);
}

static int ef_parse_duration(PyObject *v, const char *name, double *out) {
    if (PyFloat_CheckExact(v)) {
        double d = PyFloat_AS_DOUBLE(v);
        if (!(d >= 0.0 && isfinite(d))) {
            PyErr_Format(PyExc_ValueError, "%s must be a finite non-negative number", name);
            return -1;
        }
        *out = d;
        return 0;
    }
    if (PyBool_Check(v) || (!PyLong_Check(v) && !PyFloat_Check(v))) {
        PyErr_Format(PyExc_TypeError, "%s must be a finite non-negative number", name);
        return -1;
    }
    double d = PyFloat_AsDouble(v);
    if (d == -1.0 && PyErr_Occurred()) {
        return -1;
    }
    if (!(d >= 0.0 && isfinite(d))) {
        PyErr_Format(PyExc_ValueError, "%s must be a finite non-negative number", name);
        return -1;
    }
    *out = d;
    return 0;
}

static int ef_parse_nonneg_i64(PyObject *v, const char *err_msg, int64_t *out) {
    if (PyBool_Check(v) || !PyLong_Check(v)) {
        PyErr_SetString(PyExc_ValueError, err_msg);
        return -1;
    }
    int overflow = 0;
    long long val = PyLong_AsLongLongAndOverflow(v, &overflow);
    if (overflow < 0 || (overflow == 0 && val < 0)) {
        PyErr_SetString(PyExc_ValueError, err_msg);
        return -1;
    }
    if (overflow > 0) {
        PyErr_SetString(PyExc_OverflowError, err_msg);
        return -1;
    }
    *out = (int64_t)val;
    return 0;
}

static int op_stats_grow(OperationStatsCoreObject *self) {
    Py_ssize_t new_cap;
    if (self->capacity == 0) {
        new_cap = self->max_operations < 8 ? self->max_operations : 8;
    } else {
        if (self->capacity > (PY_SSIZE_T_MAX / 2)) {
            PyErr_NoMemory();
            return -1;
        }
        new_cap = self->capacity * 2;
        if (new_cap > self->max_operations) {
            new_cap = self->max_operations;
        }
    }
    if (new_cap <= self->capacity) {
        return 0;
    }
    Py_ssize_t bucket_cap = 16;
    while (bucket_cap < new_cap * 2) {
        if (bucket_cap > (PY_SSIZE_T_MAX / 2)) {
            PyErr_NoMemory();
            return -1;
        }
        bucket_cap *= 2;
    }
    OpEntry *new_entries = (OpEntry *)PyMem_Realloc(self->entries, (size_t)new_cap * sizeof(OpEntry));
    if (new_entries == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    self->entries = new_entries;
    self->capacity = new_cap;

    Py_ssize_t *new_buckets = (Py_ssize_t *)PyMem_Calloc((size_t)bucket_cap, sizeof(Py_ssize_t));
    if (new_buckets == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    Py_ssize_t new_mask = bucket_cap - 1;
    for (Py_ssize_t i = 0; i < self->count; ++i) {
        Py_ssize_t slot = (Py_ssize_t)((size_t)self->entries[i].hash & (size_t)new_mask);
        while (new_buckets[slot] != 0) {
            slot = (slot + 1) & new_mask;
        }
        new_buckets[slot] = i + 1;
    }
    PyMem_Free(self->buckets);
    self->buckets = new_buckets;
    self->mask = new_mask;
    return 0;
}

static inline Py_ssize_t op_stats_find(
    OperationStatsCoreObject *self,
    PyObject *key,
    Py_hash_t h,
    Py_ssize_t *empty_slot_out
) {
    if (self->buckets == NULL) {
        *empty_slot_out = -1;
        return -1;
    }
    Py_ssize_t mask = self->mask;
    Py_ssize_t slot = (Py_ssize_t)((size_t)h & (size_t)mask);
    Py_ssize_t idx;
    while ((idx = self->buckets[slot]) != 0) {
        OpEntry *e = &self->entries[idx - 1];
        if (
            e->name == key
            || (
                e->hash == h
                && PyUnicode_GET_LENGTH(e->name) == PyUnicode_GET_LENGTH(key)
                && PyUnicode_Compare(e->name, key) == 0
            )
        ) {
            return idx - 1;
        }
        slot = (slot + 1) & mask;
    }
    *empty_slot_out = slot;
    return -1;
}

static int OperationStatsCore_traverse(OperationStatsCoreObject *self, visitproc visit, void *arg) {
    Py_VISIT(Py_TYPE(self));
    for (Py_ssize_t i = 0; i < self->count; ++i) {
        Py_VISIT(self->entries[i].name);
    }
    return 0;
}

static int OperationStatsCore_clear(OperationStatsCoreObject *self) {
    for (Py_ssize_t i = 0; i < self->count; ++i) {
        Py_CLEAR(self->entries[i].name);
    }
    self->count = 0;
    if (self->buckets != NULL) {
        memset(self->buckets, 0, (size_t)(self->mask + 1) * sizeof(Py_ssize_t));
    }
    return 0;
}

static void OperationStatsCore_dealloc(OperationStatsCoreObject *self) {
    PyObject_GC_UnTrack(self);
    if (self->lock != NULL) {
        PyThread_free_lock(self->lock);
        self->lock = NULL;
    }
    OperationStatsCore_clear(self);
    if (self->entries != NULL) {
        PyMem_Free(self->entries);
        self->entries = NULL;
    }
    if (self->buckets != NULL) {
        PyMem_Free(self->buckets);
        self->buckets = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int OperationStatsCore_init(OperationStatsCoreObject *self, PyObject *args, PyObject *kwds) {
    static char *kwlist[] = {"max_operations", NULL};
    Py_ssize_t max_ops = 128;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|n:OperationStatsCore", kwlist, &max_ops)) {
        return -1;
    }
    if (max_ops < 1) {
        PyErr_SetString(PyExc_ValueError, "max_operations must be positive");
        return -1;
    }
    self->state = get_core_state(PyType_GetModule(Py_TYPE(self)));
    if (self->lock == NULL) {
        self->lock = PyThread_allocate_lock();
        if (self->lock == NULL) {
            PyErr_NoMemory();
            return -1;
        }
    } else {
        op_stats_lock_acquire(self);
        OperationStatsCore_clear(self);
        op_stats_lock_release(self);
    }
    self->max_operations = max_ops;
    return 0;
}

static PyObject *OperationStatsCore_record(
    OperationStatsCoreObject *self,
    PyObject *const *args,
    Py_ssize_t nargs
) {
    if (nargs != 9) {
        PyErr_SetString(
            PyExc_TypeError,
            "OperationStatsCore.record expects exactly 9 positional arguments"
        );
        return NULL;
    }
    CoreState *st = self->state;
    PyObject *orig_name = args[0];
    if (!PyUnicode_Check(orig_name) || PyUnicode_GET_LENGTH(orig_name) == 0) {
        PyErr_SetString(PyExc_ValueError, "operation name must be a non-empty string");
        return NULL;
    }

    PyObject *status_obj = args[1];
    int status_code;
    if (status_obj == st->k_success) {
        status_code = 0;
    } else if (status_obj == st->k_error) {
        status_code = 1;
    } else if (status_obj == st->k_cancelled) {
        status_code = 2;
    } else if (PyUnicode_Check(status_obj)) {
        int r = PyObject_RichCompareBool(status_obj, st->k_success, Py_EQ);
        if (r < 0) return NULL;
        if (r) {
            status_code = 0;
        } else {
            r = PyObject_RichCompareBool(status_obj, st->k_error, Py_EQ);
            if (r < 0) return NULL;
            if (r) {
                status_code = 1;
            } else {
                r = PyObject_RichCompareBool(status_obj, st->k_cancelled, Py_EQ);
                if (r < 0) return NULL;
                if (r) {
                    status_code = 2;
                } else {
                    PyErr_SetString(PyExc_ValueError, "status must be success, error, or cancelled");
                    return NULL;
                }
            }
        }
    } else {
        PyErr_SetString(PyExc_ValueError, "status must be success, error, or cancelled");
        return NULL;
    }

    double elapsed = 0.0;
    if (ef_parse_duration(args[2], "elapsed", &elapsed) < 0) {
        return NULL;
    }
    double shutdown_seconds = 0.0;
    if (ef_parse_duration(args[8], "shutdown_seconds", &shutdown_seconds) < 0) {
        return NULL;
    }

    const char *count_err = "drop, retry, rejection and timeout counts must be non-negative integers";
    int64_t dropped = 0, retries = 0, rejected = 0, timeouts = 0;
    if (args[4] != st->zero_int && ef_parse_nonneg_i64(args[4], count_err, &dropped) < 0) return NULL;
    if (args[5] != st->zero_int && ef_parse_nonneg_i64(args[5], count_err, &retries) < 0) return NULL;
    if (args[6] != st->zero_int && ef_parse_nonneg_i64(args[6], count_err, &rejected) < 0) return NULL;
    if (args[7] != st->zero_int && ef_parse_nonneg_i64(args[7], count_err, &timeouts) < 0) return NULL;

    int has_queue_depth = 0;
    int64_t queue_depth = 0;
    if (args[3] != Py_None) {
        has_queue_depth = 1;
        if (args[3] != st->zero_int) {
            if (ef_parse_nonneg_i64(
                    args[3], "queue_depth must be a non-negative integer or None", &queue_depth
                ) < 0) {
                return NULL;
            }
        }
    }

    PyObject *key;
    Py_hash_t h;
    if (PyUnicode_GET_LENGTH(orig_name) > 128) {
        key = st->k_other;
        h = st->h_other;
    } else {
        key = orig_name;
        h = PyObject_Hash(key);
        if (h == -1) return NULL;
    }

    op_stats_lock_acquire(self);

    Py_ssize_t slot = -1;
    Py_ssize_t idx = op_stats_find(self, key, h, &slot);
    if (idx < 0) {
        if (self->max_operations == 1 || self->count >= self->max_operations - 1) {
            if (key != st->k_other) {
                key = st->k_other;
                h = st->h_other;
                idx = op_stats_find(self, key, h, &slot);
            }
        }
    }
    if (idx < 0) {
        if (self->count >= self->capacity) {
            if (op_stats_grow(self) < 0) {
                op_stats_lock_release(self);
                return NULL;
            }
            slot = (Py_ssize_t)((size_t)h & (size_t)self->mask);
            while (self->buckets[slot] != 0) {
                slot = (slot + 1) & self->mask;
            }
        }
        idx = self->count++;
        OpEntry *e = &self->entries[idx];
        memset(e, 0, sizeof(OpEntry));
        e->name = Py_NewRef(key);
        e->hash = h;
        self->buckets[slot] = idx + 1;
    }

    OpEntry *e = &self->entries[idx];
    e->calls += 1;
    if (status_code == 0) {
        e->successful += 1;
    } else if (status_code == 1) {
        e->failed += 1;
    } else {
        e->cancelled += 1;
    }
    e->total_seconds += elapsed;
    if (elapsed > e->max_seconds) {
        e->max_seconds = elapsed;
    }
    if (has_queue_depth) {
        e->queue_depth = queue_depth;
        if (queue_depth > e->max_queue_depth) {
            e->max_queue_depth = queue_depth;
        }
    }
    if (dropped) e->dropped += dropped;
    if (retries) e->retries += retries;
    if (rejected) e->rejected += rejected;
    if (timeouts) e->timeouts += timeouts;
    if (shutdown_seconds) e->shutdown_seconds += shutdown_seconds;

    op_stats_lock_release(self);
    Py_RETURN_NONE;
}

static PyObject *OperationStatsCore_snapshot(
    OperationStatsCoreObject *self,
    PyObject *Py_UNUSED(ignored)
) {
    CoreState *st = self->state;
    if (
        st->operation_metrics_type == NULL
        || st->operation_stats_snapshot_type == NULL
        || st->mapping_proxy_type == NULL
    ) {
        PyErr_SetString(PyExc_RuntimeError, "eventflow._core observability is not configured");
        return NULL;
    }

    OpEntry stack_buf[16];
    OpEntry *snap = stack_buf;
    Py_ssize_t snap_cap = 16;

    op_stats_lock_acquire(self);
    Py_ssize_t n = self->count;
    if (n > snap_cap) {
        OpEntry *heap_buf = (OpEntry *)PyMem_Malloc((size_t)n * sizeof(OpEntry));
        if (heap_buf == NULL) {
            op_stats_lock_release(self);
            return PyErr_NoMemory();
        }
        snap = heap_buf;
    }
    if (n > 0) {
        memcpy(snap, self->entries, (size_t)n * sizeof(OpEntry));
        for (Py_ssize_t i = 0; i < n; ++i) {
            Py_INCREF(snap[i].name);
        }
    }
    op_stats_lock_release(self);

    PyObject *dict = PyDict_New();
    if (dict == NULL) {
        for (Py_ssize_t i = 0; i < n; ++i) {
            Py_DECREF(snap[i].name);
        }
        if (snap != stack_buf) PyMem_Free(snap);
        return NULL;
    }

    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject *args_tuple = PyTuple_New(13);
        if (args_tuple == NULL) {
            for (Py_ssize_t j = i; j < n; ++j) Py_DECREF(snap[j].name);
            if (snap != stack_buf) PyMem_Free(snap);
            Py_DECREF(dict);
            return NULL;
        }
        PyObject *vals[13];
        vals[0] = PyLong_FromLongLong(snap[i].calls);
        vals[1] = PyLong_FromLongLong(snap[i].successful);
        vals[2] = PyLong_FromLongLong(snap[i].failed);
        vals[3] = PyLong_FromLongLong(snap[i].cancelled);
        vals[4] = PyFloat_FromDouble(snap[i].total_seconds);
        vals[5] = PyFloat_FromDouble(snap[i].max_seconds);
        vals[6] = PyLong_FromLongLong(snap[i].queue_depth);
        vals[7] = PyLong_FromLongLong(snap[i].max_queue_depth);
        vals[8] = PyLong_FromLongLong(snap[i].dropped);
        vals[9] = PyLong_FromLongLong(snap[i].retries);
        vals[10] = PyLong_FromLongLong(snap[i].rejected);
        vals[11] = PyLong_FromLongLong(snap[i].timeouts);
        vals[12] = PyFloat_FromDouble(snap[i].shutdown_seconds);
        int ok = 1;
        for (int k = 0; k < 13; ++k) {
            if (vals[k] == NULL) {
                ok = 0;
            }
            PyTuple_SET_ITEM(args_tuple, k, vals[k]);
        }
        if (!ok) {
            Py_DECREF(args_tuple);
            for (Py_ssize_t j = i; j < n; ++j) Py_DECREF(snap[j].name);
            if (snap != stack_buf) PyMem_Free(snap);
            Py_DECREF(dict);
            return NULL;
        }
        PyObject *metrics = PyObject_CallObject(st->operation_metrics_type, args_tuple);
        Py_DECREF(args_tuple);
        if (metrics == NULL) {
            for (Py_ssize_t j = i; j < n; ++j) Py_DECREF(snap[j].name);
            if (snap != stack_buf) PyMem_Free(snap);
            Py_DECREF(dict);
            return NULL;
        }
        int set_rc = PyDict_SetItem(dict, snap[i].name, metrics);
        Py_DECREF(metrics);
        Py_DECREF(snap[i].name);
        if (set_rc < 0) {
            for (Py_ssize_t j = i + 1; j < n; ++j) Py_DECREF(snap[j].name);
            if (snap != stack_buf) PyMem_Free(snap);
            Py_DECREF(dict);
            return NULL;
        }
    }
    if (snap != stack_buf) {
        PyMem_Free(snap);
    }

    PyObject *proxy = PyObject_CallOneArg(st->mapping_proxy_type, dict);
    Py_DECREF(dict);
    if (proxy == NULL) {
        return NULL;
    }
    PyObject *result = PyObject_CallOneArg(st->operation_stats_snapshot_type, proxy);
    Py_DECREF(proxy);
    return result;
}

static PyObject *OperationStatsCore_reset(
    OperationStatsCoreObject *self,
    PyObject *Py_UNUSED(ignored)
) {
    PyObject *stack_names[16];
    PyObject **old_names = stack_names;
    Py_ssize_t n = 0;
    Py_ssize_t heap_allocated = 0;

    op_stats_lock_acquire(self);
    n = self->count;
    if (n > 0) {
        if (n <= 16) {
            for (Py_ssize_t i = 0; i < n; ++i) {
                old_names[i] = self->entries[i].name;
                self->entries[i].name = NULL;
            }
        } else {
            PyObject **heap_names = (PyObject **)PyMem_Malloc((size_t)n * sizeof(PyObject *));
            if (heap_names != NULL) {
                old_names = heap_names;
                heap_allocated = 1;
                for (Py_ssize_t i = 0; i < n; ++i) {
                    old_names[i] = self->entries[i].name;
                    self->entries[i].name = NULL;
                }
            } else {
                for (Py_ssize_t i = 0; i < n; ++i) {
                    Py_CLEAR(self->entries[i].name);
                }
                n = 0;
            }
        }
        self->count = 0;
        if (self->buckets != NULL) {
            memset(self->buckets, 0, (size_t)(self->mask + 1) * sizeof(Py_ssize_t));
        }
    }
    op_stats_lock_release(self);

    for (Py_ssize_t i = 0; i < n; ++i) {
        Py_DECREF(old_names[i]);
    }
    if (heap_allocated) {
        PyMem_Free(old_names);
    }
    Py_RETURN_NONE;
}

static PyMethodDef OperationStatsCore_methods[] = {
    {"record", (PyCFunction)(void (*)(void))OperationStatsCore_record, METH_FASTCALL, NULL},
    {"snapshot", (PyCFunction)OperationStatsCore_snapshot, METH_NOARGS, NULL},
    {"reset", (PyCFunction)OperationStatsCore_reset, METH_NOARGS, NULL},
    {NULL, NULL, 0, NULL},
};

static PyType_Slot OperationStatsCore_slots[] = {
    {Py_tp_doc, "Native storage and validation core for OperationStats."},
    {Py_tp_init, (void *)OperationStatsCore_init},
    {Py_tp_dealloc, (void *)OperationStatsCore_dealloc},
    {Py_tp_traverse, (void *)OperationStatsCore_traverse},
    {Py_tp_clear, (void *)OperationStatsCore_clear},
    {Py_tp_methods, OperationStatsCore_methods},
    {0, NULL},
};

static PyType_Spec OperationStatsCore_spec = {
    .name = "eventflow._core.OperationStatsCore",
    .basicsize = sizeof(OperationStatsCoreObject),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = OperationStatsCore_slots,
};

/* =========================== Module & Config ========================== */

static PyObject *configure(PyObject *module, PyObject *args) {
    CoreState *st = get_core_state(module);
    PyObject *publish_result, *stats_cls, *filter_cls, *closed_err, *invalid_err;
    PyObject *err_ctx, *exc_group, *lock_fn, *rlock_tp, *is_aw_fn, *close_aw_fn;
    if (!PyArg_ParseTuple(
            args, "OOOOOOOOOOO:configure",
            &publish_result, &stats_cls, &filter_cls, &closed_err, &invalid_err,
            &err_ctx, &exc_group, &lock_fn, &rlock_tp, &is_aw_fn, &close_aw_fn)) {
        return NULL;
    }

    PyObject *dm = PyObject_GetAttrString(publish_result, "matched");
    PyObject *dd = PyObject_GetAttrString(publish_result, "delivered");
    PyObject *df = PyObject_GetAttrString(publish_result, "failed");
    PyObject *de = PyObject_GetAttrString(publish_result, "errors");
    if (dm == NULL || dd == NULL || df == NULL || de == NULL) {
        Py_XDECREF(dm);
        Py_XDECREF(dd);
        Py_XDECREF(df);
        Py_XDECREF(de);
        return NULL;
    }

    PyObject *empty_tuple = PyTuple_New(0);
    if (empty_tuple == NULL) {
        Py_DECREF(dm);
        Py_DECREF(dd);
        Py_DECREF(df);
        Py_DECREF(de);
        return NULL;
    }

    Py_XSETREF(st->publish_result_type, Py_NewRef(publish_result));
    Py_XSETREF(st->descr_matched, dm);
    Py_XSETREF(st->descr_delivered, dd);
    Py_XSETREF(st->descr_failed, df);
    Py_XSETREF(st->descr_errors, de);
    Py_XSETREF(st->empty_tuple, empty_tuple);
    Py_XSETREF(st->event_bus_stats_type, Py_NewRef(stats_cls));
    Py_XSETREF(st->event_filter_type, Py_NewRef(filter_cls));
    Py_XSETREF(st->closed_error_type, Py_NewRef(closed_err));
    Py_XSETREF(st->invalid_handler_error_type, Py_NewRef(invalid_err));
    Py_XSETREF(st->error_context_type, Py_NewRef(err_ctx));
    Py_XSETREF(st->exception_group_type, Py_NewRef(exc_group));
    Py_XSETREF(st->threading_lock_fn, Py_NewRef(lock_fn));
    Py_XSETREF(st->rlock_type, Py_NewRef(rlock_tp));
    Py_XSETREF(st->is_awaitable_fn, Py_NewRef(is_aw_fn));
    Py_XDECREF(st->close_awaitable_fn);
    st->close_awaitable_fn = Py_NewRef(close_aw_fn);

    PyObject *empty_pr = PyObject_CallNoArgs(publish_result);
    if (empty_pr == NULL) return NULL;
    Py_XSETREF(st->empty_publish_result, empty_pr);

    Py_RETURN_NONE;
}

static PyObject *configure_observability(PyObject *module, PyObject *args) {
    CoreState *st = get_core_state(module);
    PyObject *metrics_tp, *snapshot_tp, *proxy_tp;
    if (!PyArg_ParseTuple(
            args, "OOO:configure_observability", &metrics_tp, &snapshot_tp, &proxy_tp
        )) {
        return NULL;
    }
    Py_XSETREF(st->operation_metrics_type, Py_NewRef(metrics_tp));
    Py_XSETREF(st->operation_stats_snapshot_type, Py_NewRef(snapshot_tp));
    Py_XSETREF(st->mapping_proxy_type, Py_NewRef(proxy_tp));
    Py_RETURN_NONE;
}

static int core_traverse(PyObject *m, visitproc visit, void *arg) {
    CoreState *st = get_core_state(m);
    Py_VISIT(st->flag_type);
    Py_VISIT(st->subscriber_type);
    Py_VISIT(st->bus_core_type);
    Py_VISIT(st->op_stats_core_type);
    Py_VISIT(st->publish_result_type);
    Py_VISIT(st->descr_matched);
    Py_VISIT(st->descr_delivered);
    Py_VISIT(st->descr_failed);
    Py_VISIT(st->descr_errors);
    Py_VISIT(st->empty_publish_result);
    Py_VISIT(st->empty_tuple);
    Py_VISIT(st->event_bus_stats_type);
    Py_VISIT(st->event_filter_type);
    Py_VISIT(st->closed_error_type);
    Py_VISIT(st->invalid_handler_error_type);
    Py_VISIT(st->error_context_type);
    Py_VISIT(st->exception_group_type);
    Py_VISIT(st->threading_lock_fn);
    Py_VISIT(st->rlock_type);
    Py_VISIT(st->operation_metrics_type);
    Py_VISIT(st->operation_stats_snapshot_type);
    Py_VISIT(st->mapping_proxy_type);
    Py_VISIT(st->is_awaitable_fn);
    Py_VISIT(st->close_awaitable_fn);
    Py_VISIT(st->zero_int);
    Py_VISIT(st->k__lock);
    Py_VISIT(st->k__active);
    Py_VISIT(st->k__consume);
    Py_VISIT(st->k__subscriber_snapshot);
    Py_VISIT(st->k__publish_python);
    Py_VISIT(st->k_cancel);
    Py_VISIT(st->k_close);
    Py_VISIT(st->k_notify_all);
    Py_VISIT(st->k_matches);
    Py_VISIT(st->k_acquire);
    Py_VISIT(st->k_release);
    Py_VISIT(st->k_append);
    Py_VISIT(st->k_clear);
    Py_VISIT(st->k___enter__);
    Py_VISIT(st->k___exit__);
    Py_VISIT(st->k_success);
    Py_VISIT(st->k_error);
    Py_VISIT(st->k_cancelled);
    Py_VISIT(st->k_other);
    return 0;
}

static int core_clear(PyObject *m) {
    CoreState *st = get_core_state(m);
    Py_CLEAR(st->flag_type);
    Py_CLEAR(st->subscriber_type);
    Py_CLEAR(st->bus_core_type);
    Py_CLEAR(st->op_stats_core_type);
    Py_CLEAR(st->publish_result_type);
    Py_CLEAR(st->descr_matched);
    Py_CLEAR(st->descr_delivered);
    Py_CLEAR(st->descr_failed);
    Py_CLEAR(st->descr_errors);
    Py_CLEAR(st->empty_publish_result);
    Py_CLEAR(st->empty_tuple);
    Py_CLEAR(st->event_bus_stats_type);
    Py_CLEAR(st->event_filter_type);
    Py_CLEAR(st->closed_error_type);
    Py_CLEAR(st->invalid_handler_error_type);
    Py_CLEAR(st->error_context_type);
    Py_CLEAR(st->exception_group_type);
    Py_CLEAR(st->threading_lock_fn);
    Py_CLEAR(st->rlock_type);
    Py_CLEAR(st->operation_metrics_type);
    Py_CLEAR(st->operation_stats_snapshot_type);
    Py_CLEAR(st->mapping_proxy_type);
    Py_CLEAR(st->is_awaitable_fn);
    Py_CLEAR(st->close_awaitable_fn);
    Py_CLEAR(st->zero_int);
    Py_CLEAR(st->k__lock);
    Py_CLEAR(st->k__active);
    Py_CLEAR(st->k__consume);
    Py_CLEAR(st->k__subscriber_snapshot);
    Py_CLEAR(st->k__publish_python);
    Py_CLEAR(st->k_cancel);
    Py_CLEAR(st->k_close);
    Py_CLEAR(st->k_notify_all);
    Py_CLEAR(st->k_matches);
    Py_CLEAR(st->k_acquire);
    Py_CLEAR(st->k_release);
    Py_CLEAR(st->k_append);
    Py_CLEAR(st->k_clear);
    Py_CLEAR(st->k___enter__);
    Py_CLEAR(st->k___exit__);
    Py_CLEAR(st->k_success);
    Py_CLEAR(st->k_error);
    Py_CLEAR(st->k_cancelled);
    Py_CLEAR(st->k_other);
    return 0;
}

static void core_free(void *m) {
    core_clear((PyObject *)m);
}

static int core_exec(PyObject *m) {
    CoreState *st = get_core_state(m);
    #define INIT_INTERN(field, str) \
        st->field = PyUnicode_InternFromString(str); \
        if (st->field == NULL) return -1;

    INIT_INTERN(k__lock, "_lock");
    INIT_INTERN(k__active, "_active");
    INIT_INTERN(k__consume, "_consume");
    INIT_INTERN(k__subscriber_snapshot, "_subscriber_snapshot");
    INIT_INTERN(k__publish_python, "_publish_python");
    INIT_INTERN(k_cancel, "cancel");
    INIT_INTERN(k_close, "close");
    INIT_INTERN(k_notify_all, "notify_all");
    INIT_INTERN(k_matches, "matches");
    INIT_INTERN(k_acquire, "acquire");
    INIT_INTERN(k_release, "release");
    INIT_INTERN(k_append, "append");
    INIT_INTERN(k_clear, "clear");
    INIT_INTERN(k___enter__, "__enter__");
    INIT_INTERN(k___exit__, "__exit__");
    INIT_INTERN(k_success, "success");
    INIT_INTERN(k_error, "error");
    INIT_INTERN(k_cancelled, "cancelled");
    INIT_INTERN(k_other, "other");
    #undef INIT_INTERN

    st->zero_int = PyLong_FromLong(0);
    if (st->zero_int == NULL) return -1;
    st->h_other = PyObject_Hash(st->k_other);
    if (st->h_other == -1) return -1;

    st->flag_type = (PyTypeObject *)PyType_FromModuleAndSpec(m, &Flag_spec, NULL);
    if (st->flag_type == NULL) return -1;
    st->subscriber_type = (PyTypeObject *)PyType_FromModuleAndSpec(m, &Subscriber_spec, NULL);
    if (st->subscriber_type == NULL) return -1;
    st->bus_core_type = (PyTypeObject *)PyType_FromModuleAndSpec(m, &BusCore_spec, NULL);
    if (st->bus_core_type == NULL) return -1;
    st->op_stats_core_type = (PyTypeObject *)PyType_FromModuleAndSpec(m, &OperationStatsCore_spec, NULL);
    if (st->op_stats_core_type == NULL) return -1;

    if (
        PyModule_AddObjectRef(m, "Flag", (PyObject *)st->flag_type) < 0
        || PyModule_AddObjectRef(m, "Subscriber", (PyObject *)st->subscriber_type) < 0
        || PyModule_AddObjectRef(m, "BusCore", (PyObject *)st->bus_core_type) < 0
        || PyModule_AddObjectRef(m, "OperationStatsCore", (PyObject *)st->op_stats_core_type) < 0
    ) {
        return -1;
    }
    return 0;
}

static PyMethodDef core_methods[] = {
    {"configure", configure, METH_VARARGS, "Bind EventFlow runtime classes."},
    {"configure_observability", configure_observability, METH_VARARGS, "Bind EventFlow observability classes."},
    {NULL, NULL, 0, NULL},
};

static PyModuleDef_Slot core_slots[] = {
    {Py_mod_exec, core_exec},
#if PY_VERSION_HEX >= 0x030D0000
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL},
};

static struct PyModuleDef core_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_core",
    .m_doc = "Optional C dispatch kernel for synchronous EventBus.",
    .m_size = sizeof(CoreState),
    .m_methods = core_methods,
    .m_slots = core_slots,
    .m_traverse = core_traverse,
    .m_clear = core_clear,
    .m_free = core_free,
};

PyMODINIT_FUNC PyInit__core(void) {
    return PyModuleDef_Init(&core_module);
}
