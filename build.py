#!/usr/bin/env python3
import os
import subprocess
import argparse
from pathlib import Path

def install_system_deps():
    """Install system dependencies via apt-get (Ubuntu 24.04)."""
    packages = [
        "build-essential", "git", "g++", "cmake", "bison", "flex",
        "libboost-all-dev", "libevent-dev", "libdouble-conversion-dev",
        "libgoogle-glog-dev", "libgflags-dev", "libiberty-dev",
        "liblz4-dev", "liblzma-dev", "libbz2-dev", "libsnappy-dev",
        "make", "zlib1g-dev", "binutils-dev", "libjemalloc-dev",
        "libssl-dev", "pkg-config", "libunwind-dev", "libelf-dev",
        "libdwarf-dev", "libsodium-dev", "libaio-dev", "libnuma-dev",
        "liburing-dev", "libfast-float-dev"
    ]
    print("Installing system dependencies with apt-get...")
    run(["apt-get", "update"])
    run(["apt-get", "install", "-y"] + packages)
    print("System dependencies installed successfully!")
    return True

def run(cmd, cwd=None):
    """Run command and exit on failure."""
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--install-system-deps", action="store_true")
    parser.add_argument("-b", "--build-deps", action="store_true")
    parser.add_argument("-p", "--prefix", default="opt/ycsb")
    parser.add_argument("-j", "--jobs", type=int, default=1)
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    prefix = Path(args.prefix).resolve()
    external_dir = script_dir / "ext"
    build_type = "RelWithDebInfo" if args.debug else "Release"
    
    # Setup environment
    env_paths = {
        'CMAKE_PREFIX_PATH': f"{prefix}/lib/cmake:{prefix}/lib64/cmake:{prefix}",
        'PKG_CONFIG_PATH': f"{prefix}/lib/pkgconfig:{prefix}/lib64/pkgconfig",
        'LD_LIBRARY_PATH': f"{prefix}/lib:{prefix}/lib64",
        'LIBRARY_PATH': f"{prefix}/lib:{prefix}/lib64",
        'PATH': f"{prefix}/bin:{os.environ.get('PATH', '')}"
    }
    for k, v in env_paths.items():
        os.environ[k] = f"{v}:{os.environ.get(k, '')}"

    cmake_flags = [
        f"-DCMAKE_INSTALL_PREFIX={prefix}",
        f"-DCMAKE_MODULE_PATH={prefix}/lib/cmake;{script_dir}/ext/CacheLib-Holpaca/cachelib/cmake",
        "-DBUILD_SHARED_LIBS=ON",
        f"-DCMAKE_BUILD_TYPE={build_type}",
        f"-DCMAKE_VERBOSE_MAKEFILE={'ON' if args.verbose else 'OFF'}",
        f"-DCMAKE_INSTALL_RPATH={prefix}/lib:{prefix}/lib64",
        f"-DCMAKE_INSTALL_RPATH_USE_LINK_PATH=FALSE",
        f"-DCMAKE_BUILD_WITH_INSTALL_RPATH=TRUE",
    ]

    # Dependencies: name, repo, branch/tag, commit (optional), git_flags, source_dir, cmake_flags
    deps = [
                    ("zstd", "https://github.com/facebook/zstd", "v1.5.6", None, [], "build/cmake", ["-DZSTD_BUILD_TESTS=OFF"]),
                    ("googletest", "https://github.com/google/googletest", "v1.15.2", None, [], ".", []),
                    ("glog", "https://github.com/google/glog", "v0.5.0", None, [], ".", ["-DWITH_GFLAGS=OFF"]),
                    ("gflags", "https://github.com/gflags/gflags", "v2.2.2", None, [], ".", ["-DGFLAGS_BUILD_TESTING=NO"]),
                    ("fmt", "https://github.com/fmtlib/fmt", "10.2.1", None, [], ".", ["-DFMT_TEST=NO"]),
                    ("sparsemap", "https://github.com/Tessil/sparse-map", "v0.6.2", None, ['--recurse-submodules', '--shallow-submodules'], ".", []),
                    ("folly", "https://github.com/facebook/folly", "main", "17be1d", ['--recurse-submodules', '--shallow-submodules'], ".", ["-DBUILD_TESTS=OFF"]),
                    ("fizz", "https://github.com/facebookincubator/fizz", "main", "5576ab83", ['--recurse-submodules', '--shallow-submodules'], "fizz", ["-DBUILD_TESTS=OFF"]),
                    ("wangle", "https://github.com/facebook/wangle", "main", "0c80d9e", ['--recurse-submodules', '--shallow-submodules'], "wangle", ["-DBUILD_TESTS=OFF"]),
                    ("mvfst", "https://github.com/facebook/mvfst", "main", "aa7ac3", ['--recurse-submodules', '--shallow-submodules'], ".", []),
                    ("fbthrift", "https://github.com/facebook/fbthrift", "main", "c21dccc", ['--recurse-submodules', '--shallow-submodules'], ".", ["-DCMAKE_BUILD_WITH_INSTALL_RPATH=FALSE"]),
                    ("grpc", "https://github.com/grpc/grpc", "v1.50.1", None, ['--recurse-submodules', '--shallow-submodules'], ".", ["-DgRPC_INSTALL=ON", "-DgRPC_BUILD_TESTS=OFF", "-DgRPC_ZLIB_PROVIDER=package", "-DgRPC_SSL_PROVIDER=package", "-DABSL_PROPAGATE_CXX_STD=ON", "-Dprotobuf_WITH_ZLIB=ON", "-DCMAKE_BUILD_WITH_INSTALL_RPATH=FALSE"]),
                    ("Shards", "https://github.com/vaavaav/SHARDS-cpp", "varying-size", None, [], ".", []),
                    ("gsl", "https://github.com/ampl/gsl", "20211111", None, [], ".", ["-DGSL_DISABLE_TESTS=1", "-DDOCUMENTATION=OFF", "-DNO_AMPL_BINDINGS=1"]),
                    ("CacheLib-Holpaca", "https://github.com/vaavaav/CacheLib-Holpaca", "holpaca-2404", None, [], "cachelib", ["-DBUILD_TESTS=OFF", "-DCMAKE_FIND_DEBUG_MODE=ON"]),
         ("rocksdb", "https://github.com/vaavaav/rocksdb", "main", None, [], ".", ["-DWITH_TESTS=OFF", "-DWITH_GFLAGS=OFF"]),
    ]

    # Install system dependencies if requested
    if args.install_system_deps:
        if not install_system_deps():
            print("Failed to install system dependencies")
        return

    if args.build_deps:
        external_dir.mkdir(parents=True, exist_ok=True)
        
        # Clone and build each dependency
        for name, repo, branch_tag, commit, extra_git_flags, src_dir, extra_flags in deps:
            target = external_dir / name
            if target.exists():
                run(["rm", "-rf", str(target)])
            
            # Clone
            run(["git", "clone", "-b", branch_tag, "--depth", "1"] + extra_git_flags + [str(repo), str(target)])
            
            # Checkout specific commit if provided
            if commit:
                run(["git", "fetch", "--unshallow"], cwd=target)  # Need full history for commit
                run(["git", "checkout", commit], cwd=target)
            
            # Build
            build_dir = f"build-{name}"
            source_path = target / src_dir
            run(["cmake"] + cmake_flags + extra_flags + [f"-B{build_dir}", f"-S{source_path}"])
            run(["make", f"-C{build_dir}", f"-j{args.jobs}", "install"])

    # Build main project
    run(["cmake"] + cmake_flags + ["-Bbuild-ycsb", "-S."])
    run(["make", "-Cbuild-ycsb", f"-j{args.jobs}", "install"])
    
    print("Build completed!")

if __name__ == "__main__":
    main()
