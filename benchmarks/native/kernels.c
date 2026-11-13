#define PY_SSIZE_T_CLEAN
#include <Python.h>

static PyObject *filter_by_type(PyObject *self, PyObject *args) {
    PyObject *events;
    PyObject *event_type;
    PyObject *limit = Py_None;
    PyObject *iterator;
    PyObject *matched;
    PyObject *event;
    Py_ssize_t maximum_matches = -1;

    if (!PyArg_ParseTuple(args, "OO|O:filter_by_type", &events, &event_type, &limit)) {
        return NULL;
    }
    if (limit != Py_None) {
        PyObject *zero;
        PyObject *event_count;
        int is_positive;
        int limit_covers_history;
        if (PyBool_Check(limit) || !PyLong_Check(limit)) {
            PyErr_SetString(PyExc_ValueError, "history query limit must be a positive integer");
            return NULL;
        }
        if (!PyTuple_Check(events)) {
            PyErr_SetString(PyExc_TypeError, "limited type filtering requires a tuple snapshot");
            return NULL;
        }
        zero = PyLong_FromLong(0);
        if (zero == NULL) {
            return NULL;
        }
        is_positive = PyObject_RichCompareBool(limit, zero, Py_GT);
        Py_DECREF(zero);
        if (is_positive < 0) {
            return NULL;
        }
        if (!is_positive) {
            PyErr_SetString(PyExc_ValueError, "history query limit must be a positive integer");
            return NULL;
        }
        event_count = PyLong_FromSsize_t(PyTuple_GET_SIZE(events));
        if (event_count == NULL) {
            return NULL;
        }
        limit_covers_history = PyObject_RichCompareBool(limit, event_count, Py_GE);
        Py_DECREF(event_count);
        if (limit_covers_history < 0) {
            return NULL;
        }
        if (!limit_covers_history) {
            maximum_matches = PyLong_AsSsize_t(limit);
            if (maximum_matches < 0 || PyErr_Occurred()) {
                PyErr_Clear();
                PyErr_SetString(PyExc_ValueError, "history query limit must be a positive integer");
                return NULL;
            }
        } else {
            maximum_matches = PyTuple_GET_SIZE(events);
        }
    }

    matched = PyList_New(0);
    if (matched == NULL) {
        return NULL;
    }
    if (limit != Py_None) {
        Py_ssize_t index;
        for (index = PyTuple_GET_SIZE(events); index > 0; --index) {
            event = PyTuple_GET_ITEM(events, index - 1);
            int is_match = PyObject_IsInstance(event, event_type);
            if (is_match < 0 || (is_match && PyList_Append(matched, event) < 0)) {
                Py_DECREF(matched);
                return NULL;
            }
            if (is_match && PyList_GET_SIZE(matched) == maximum_matches) {
                break;
            }
        }
        if (PyList_Reverse(matched) < 0) {
            Py_DECREF(matched);
            return NULL;
        }
        PyObject *result = PyList_AsTuple(matched);
        Py_DECREF(matched);
        return result;
    }

    iterator = PyObject_GetIter(events);
    if (iterator == NULL) {
        Py_DECREF(matched);
        return NULL;
    }

    while ((event = PyIter_Next(iterator)) != NULL) {
        int is_match = PyObject_IsInstance(event, event_type);
        if (is_match < 0 || (is_match && PyList_Append(matched, event) < 0)) {
            Py_DECREF(event);
            Py_DECREF(iterator);
            Py_DECREF(matched);
            return NULL;
        }
        Py_DECREF(event);
    }
    Py_DECREF(iterator);
    if (PyErr_Occurred()) {
        Py_DECREF(matched);
        return NULL;
    }

    PyObject *result = PyList_AsTuple(matched);
    Py_DECREF(matched);
    return result;
}

static PyObject *filter_by_type_tuple(PyObject *self, PyObject *args) {
    PyObject *events;
    PyObject *event_type;
    PyObject *matched;
    Py_ssize_t index;

    if (!PyArg_ParseTuple(args, "OO:filter_by_type_tuple", &events, &event_type)) {
        return NULL;
    }
    if (!PyTuple_Check(events)) {
        PyErr_SetString(PyExc_TypeError, "tuple type filtering requires a tuple snapshot");
        return NULL;
    }

    matched = PyList_New(0);
    if (matched == NULL) {
        return NULL;
    }
    for (index = 0; index < PyTuple_GET_SIZE(events); ++index) {
        PyObject *event = PyTuple_GET_ITEM(events, index);
        int is_match = PyObject_IsInstance(event, event_type);
        if (is_match < 0 || (is_match && PyList_Append(matched, event) < 0)) {
            Py_DECREF(matched);
            return NULL;
        }
    }

    PyObject *result = PyList_AsTuple(matched);
    Py_DECREF(matched);
    return result;
}

static int subscriber_key_le(PyObject *left_key, PyObject *right_key) {
    Py_ssize_t left_id = PyLong_AsSsize_t(left_key);
    if (left_id != -1 || !PyErr_Occurred()) {
        Py_ssize_t right_id = PyLong_AsSsize_t(right_key);
        if (right_id != -1 || !PyErr_Occurred()) {
            return left_id <= right_id;
        }
    }
    if (!PyErr_ExceptionMatches(PyExc_OverflowError)) {
        return -1;
    }
    PyErr_Clear();
    return PyObject_RichCompareBool(left_key, right_key, Py_LE);
}

static PyObject *merge_subscribers(PyObject *self, PyObject *args) {
    PyObject *unkeyed;
    PyObject *keyed;
    PyObject *result = NULL;
    PyObject *left_key = NULL;
    PyObject *left_value = NULL;
    PyObject *right_key = NULL;
    PyObject *right_value = NULL;
    Py_ssize_t left_position = 0;
    Py_ssize_t right_position = 0;
    Py_ssize_t left_count;
    Py_ssize_t right_count;
    Py_ssize_t output_index = 0;
    int has_left;
    int has_right;
    int failed = 0;

    if (!PyArg_ParseTuple(args, "O!O!:merge_subscribers", &PyDict_Type, &unkeyed,
                          &PyDict_Type, &keyed)) {
        return NULL;
    }
    /* EventBus holds its own lock; critical sections also protect free-threaded dict access. */
#if PY_VERSION_HEX >= 0x030D0000
    Py_BEGIN_CRITICAL_SECTION2(unkeyed, keyed);
#endif
    left_count = PyDict_Size(unkeyed);
    right_count = PyDict_Size(keyed);
    if (left_count < 0 || right_count < 0) {
        failed = 1;
        goto unlock;
    }
    result = PyTuple_New(left_count + right_count);
    if (result == NULL) {
        failed = 1;
        goto unlock;
    }

    has_left = PyDict_Next(unkeyed, &left_position, &left_key, &left_value);
    has_right = PyDict_Next(keyed, &right_position, &right_key, &right_value);
    while (has_left && has_right) {
        int left_precedes = subscriber_key_le(left_key, right_key);
        PyObject *value;

        if (left_precedes < 0) {
            failed = 1;
            break;
        }
        if (left_precedes) {
            value = left_value;
            has_left = PyDict_Next(unkeyed, &left_position, &left_key, &left_value);
        } else {
            value = right_value;
            has_right = PyDict_Next(keyed, &right_position, &right_key, &right_value);
        }
        Py_INCREF(value);
        PyTuple_SET_ITEM(result, output_index++, value);
    }
    while (!failed && has_left) {
        Py_INCREF(left_value);
        PyTuple_SET_ITEM(result, output_index++, left_value);
        has_left = PyDict_Next(unkeyed, &left_position, &left_key, &left_value);
    }
    while (!failed && has_right) {
        Py_INCREF(right_value);
        PyTuple_SET_ITEM(result, output_index++, right_value);
        has_right = PyDict_Next(keyed, &right_position, &right_key, &right_value);
    }
    if (!failed && output_index != left_count + right_count) {
        PyErr_SetString(PyExc_RuntimeError, "subscriber maps changed during native merge");
        failed = 1;
    }

unlock:
    ;
#if PY_VERSION_HEX >= 0x030D0000
    Py_END_CRITICAL_SECTION2();
#endif
    if (failed) {
        Py_XDECREF(result);
        return NULL;
    }
    return result;
}

static PyMethodDef methods[] = {
    {"filter_by_type", filter_by_type, METH_VARARGS,
     "Filter a sequence using Python isinstance semantics."},
    {"filter_by_type_tuple", filter_by_type_tuple, METH_VARARGS,
     "Filter a tuple using direct indexed traversal."},
    {"merge_subscribers", merge_subscribers, METH_VARARGS,
     "Merge two integer-keyed subscriber maps in registration order."},
    {NULL, NULL, 0, NULL},
};

#if PY_VERSION_HEX >= 0x030D0000
static PyModuleDef_Slot module_slots[] = {
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
    {0, NULL},
};
#define MODULE_SLOTS module_slots
#else
#define MODULE_SLOTS NULL
#endif

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "eventflow_native_c",
    "Benchmark-only C kernels; not imported by EventFlow runtime modules.",
    0,
    methods,
    MODULE_SLOTS,
    NULL,
    NULL,
    NULL,
};

PyMODINIT_FUNC PyInit_eventflow_native_c(void) {
#if PY_VERSION_HEX >= 0x030D0000
    return PyModuleDef_Init(&module);
#else
    return PyModule_Create(&module);
#endif
}
