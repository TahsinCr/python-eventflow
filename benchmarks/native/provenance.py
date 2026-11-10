"""Read and verify metadata for locally built native benchmark candidates."""

from __future__ import annotations

import hashlib
import json
from importlib import import_module
from pathlib import Path


def native_build_metadata(implementation: str) -> dict[str, str]:
    module_names = {"cython": "eventflow_native_cython", "c": "eventflow_native_c"}
    module_name = module_names.get(implementation)
    if module_name is None:
        raise ValueError(f"unsupported native implementation: {implementation}")
    module = import_module(module_name)
    if module.__file__ is None:
        raise RuntimeError(f"native module {module_name} has no binary path")

    binary = Path(module.__file__).resolve()
    manifest_path = binary.parent / "native-build.json"
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    artifact = manifest["artifacts"][module_name]
    binary_sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
    if artifact["path"] != binary.name or artifact["sha256"] != binary_sha256:
        raise RuntimeError(f"native build manifest does not match {binary}")

    return {
        "eventflow_native_backend": implementation,
        "eventflow_native_binary_sha256": binary_sha256,
        "eventflow_native_build_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "eventflow_native_python": manifest["python"],
        "eventflow_native_cython": manifest["cython"],
        "eventflow_native_compiler": manifest["compiler_type"],
        "eventflow_native_compiler_version": manifest["compiler_version"],
        "eventflow_native_compiler_flags": json.dumps(
            {
                "compiler": manifest["compiler"],
                "compiler_so": manifest["compiler_so"],
                "compile_options": manifest["compile_options"],
                "linker_so": manifest["linker_so"],
                "link_options": manifest["link_options"],
                "environment": manifest["environment"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
