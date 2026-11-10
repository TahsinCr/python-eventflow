#define PY_SSIZE_T_CLEAN
#include <Python.h>

static PyObject *filter_by_type(PyObject *self, PyObject *args) {
    PyObject *events;
    PyObject *event_type;
    PyObject *iterator;
    PyObject *matched;
    PyObject *event;

    if (!PyArg_ParseTuple(args, "OO:filter_by_type", &events, &event_type)) {
        return NULL;
    }
    iterator = PyObject_GetIter(events);
    if (iterator == NULL) {
        return NULL;
    }
    matched = PyList_New(0);
    if (matched == NULL) {
        Py_DECREF(iterator);
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
        Py_ssize_t left_id = PyLong_AsSsize_t(left_key);
        Py_ssize_t right_id = PyLong_AsSsize_t(right_key);
        PyObject *value;

        if (PyErr_Occurred()) {
            failed = 1;
            break;
        }
        if (left_id <= right_id) {
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
        Py_ssize_t identifier = PyLong_AsSsize_t(left_key);
        if (PyErr_Occurred()) {
            failed = 1;
            break;
        }
        (void)identifier;
        Py_INCREF(left_value);
        PyTuple_SET_ITEM(result, output_index++, left_value);
        has_left = PyDict_Next(unkeyed, &left_position, &left_key, &left_value);
    }
    while (!failed && has_right) {
        Py_ssize_t identifier = PyLong_AsSsize_t(right_key);
        if (PyErr_Occurred()) {
            failed = 1;
            break;
        }
        (void)identifier;
        Py_INCREF(right_value);
        PyTuple_SET_ITEM(result, output_index++, right_value);
        has_right = PyDict_Next(keyed, &right_position, &right_key, &right_value);
    }
    if (!failed && output_index != left_count + right_count) {
        PyErr_SetString(PyExc_RuntimeError, "subscriber maps changed during native merge");
        failed = 1;
    }

unlock:
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
