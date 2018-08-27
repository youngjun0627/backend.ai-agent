import ctypes
import platform


# ref: https://developer.nvidia.com/cuda-toolkit-archive
TARGET_CUDA_VERSIONS = (
    (9, 2), (9, 1), (9, 0),
    (8, 0),
    (7, 5), (7, 0),
    (6, 5), (6, 0),
    (5, 5), (5, 0),
    # older versions are not supported
)


class cudaDeviceProp(ctypes.Structure):
    _fields_ = [
        ('name', ctypes.c_char * 256),
        ('totalGlobalMem', ctypes.c_size_t),
        ('sharedMemPerBlock', ctypes.c_size_t),
        ('regsPerBlock', ctypes.c_int),
        ('warpSize', ctypes.c_int),
        ('memPitch', ctypes.c_size_t),
        ('maxThreadsPerBlock', ctypes.c_int),
        ('maxThreadsDim', ctypes.c_int * 3),
        ('maxGridSize', ctypes.c_int * 3),
        ('clockRate', ctypes.c_int),
        ('totalConstMem', ctypes.c_size_t),
        ('major', ctypes.c_int),
        ('minor', ctypes.c_int),
        ('textureAlignment', ctypes.c_size_t),
        ('texturePitchAlignment', ctypes.c_size_t),
        ('deviceOverlap', ctypes.c_int),
        ('multiProcessorCount', ctypes.c_int),
        ('kernelExecTimeoutEnabled', ctypes.c_int),
        ('integrated', ctypes.c_int),
        ('canMapHostMemory', ctypes.c_int),
        ('computeMode', ctypes.c_int),
        ('maxTexture1D', ctypes.c_int),
        ('maxTexture1DMipmap', ctypes.c_int),
        ('maxTexture1DLinear', ctypes.c_int),
        ('maxTexture2D', ctypes.c_int * 2),
        ('maxTexture2DMipmap', ctypes.c_int * 2),
        ('maxTexture2DLinear', ctypes.c_int * 3),
        ('maxTexture2DGather', ctypes.c_int * 2),
        ('maxTexture3D', ctypes.c_int * 3),
        ('maxTexture3DAlt', ctypes.c_int * 3),
        ('maxTextureCubemap', ctypes.c_int),
        ('maxTexture1DLayered', ctypes.c_int * 2),
        ('maxTexture2DLayered', ctypes.c_int * 3),
        ('maxTextureCubemapLayered', ctypes.c_int * 2),
        ('maxSurface1D', ctypes.c_int),
        ('maxSurface2D', ctypes.c_int * 2),
        ('maxSurface3D', ctypes.c_int * 3),
        ('maxSurface1DLayered', ctypes.c_int * 2),
        ('maxSurface2DLayered', ctypes.c_int * 3),
        ('maxSurfaceCubemap', ctypes.c_int),
        ('maxSurfaceCubemapLayered', ctypes.c_int * 2),
        ('surfaceAlignment', ctypes.c_size_t),
        ('concurrentKernels', ctypes.c_int),
        ('ECCEnabled', ctypes.c_int),
        ('pciBusID', ctypes.c_int),
        ('pciDeviceID', ctypes.c_int),
        ('pciDomainID', ctypes.c_int),
        ('tccDriver', ctypes.c_int),
        ('asyncEngineCount', ctypes.c_int),
        ('unifiedAddressing', ctypes.c_int),
        ('memoryClockRate', ctypes.c_int),
        ('memoryBusWidth', ctypes.c_int),
        ('l2CacheSize', ctypes.c_int),
        ('maxThreadsPerMultiProcessor', ctypes.c_int),
        ('streamPrioritiesSupported', ctypes.c_int),
        ('globalL1CacheSupported', ctypes.c_int),
        ('localL1CacheSupported', ctypes.c_int),
        ('sharedMemPerMultiprocessor', ctypes.c_size_t),
        ('regsPerMultiprocessor', ctypes.c_int),
        ('managedMemSupported', ctypes.c_int),
        ('isMultiGpuBoard', ctypes.c_int),
        ('multiGpuBoardGroupID', ctypes.c_int),
        ('singleToDoublePrecisionPerfRatio', ctypes.c_int),
        ('pageableMemoryAccess', ctypes.c_int),
        ('concurrentManagedAccess', ctypes.c_int),
        ('computePreemptionSupported', ctypes.c_int),
        ('canUseHostPointerForRegisteredMem', ctypes.c_int),
        ('cooperativeLaunch', ctypes.c_int),
        ('cooperativeMultiDeviceLaunch', ctypes.c_int),
        ('pageableMemoryAccessUsesHostPageTables', ctypes.c_int),
        ('directManagedMemAccessFromHost', ctypes.c_int),
        ('_reserved', ctypes.c_int * 128),
    ]


def _load_library(name):
    try:
        if platform.system() == 'Windows':
            return ctypes.windll.LoadLibrary(name)
        else:
            return ctypes.cdll.LoadLibrary(name)
    except OSError:
        pass
    return None


def _load_libcudart():
    """
    Return the ctypes.DLL object for cudart or None
    """
    system_type = platform.system()
    if system_type == 'Windows':
        arch = platform.architecture()[0]
        for major, minor in TARGET_CUDA_VERSIONS:
            ver = f'{major}{minor}'
            cudart = _load_library('cudart%s_%d.dll' % (arch[:2], ver))
            if cudart is not None:
                return cudart
    elif system_type == 'Darwin':
        for major, minor in TARGET_CUDA_VERSIONS:
            cudart = _load_library('libcudart.%d.%d.dylib' % (major, minor))
            if cudart is not None:
                return cudart
        return _load_library('libcudart.dylib')
    else:
        for major, minor in TARGET_CUDA_VERSIONS:
            cudart = _load_library('libcudart.so.%d.%d' % (major, minor))
            if cudart is not None:
                return cudart
        return _load_library('libcudart.so')
    return None


class libcudart:

    _lib = None

    @classmethod
    def _ensure_lib(cls):
        if cls._lib is None:
            cls._lib = _load_libcudart()
        if cls._lib is None:
            raise ImportError('CUDA runtime is not available!')

    @classmethod
    def invoke_lib(cls, func_name, *args):
        cls._ensure_lib()
        func = getattr(cls._lib, func_name)
        rc = func(*args)
        if rc != 0:
            raise RuntimeError(f'CUDA API error: {func_name}() returned {rc}')
        return 0

    @classmethod
    def get_device_count(cls) -> int:
        count = ctypes.c_int()
        try:
            cls.invoke_lib('cudaGetDeviceCount', ctypes.byref(count))
        except ImportError:
            return 0
        return count.value

    @classmethod
    def get_version(cls) -> int:
        version = ctypes.c_int()
        try:
            cls.invoke_lib('cudaRuntimeGetVersion', ctypes.byref(version))
        except ImportError:
            return 0
        return version.value

    @classmethod
    def get_device_props(cls, dev_idx: int):
        props = cudaDeviceProp()
        cls.invoke_lib('cudaGetDeviceProperties', ctypes.byref(props), dev_idx)
        props = {
            k: getattr(props, k) for k, _ in cudaDeviceProp._fields_
        }
        pci_bus_id = b' ' * 16
        cls.invoke_lib('cudaDeviceGetPCIBusId',
                       ctypes.c_char_p(pci_bus_id), 16, dev_idx)
        props['name'] = props['name'].decode()
        props['pciBusID_str'] = pci_bus_id.split(b'\x00')[0].decode()
        return props
