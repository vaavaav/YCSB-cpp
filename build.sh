#!/bin/bash
set -u

JOBS=1
PREFIX=$PWD/opt/ycsb
CMAKE_BUILD_TYPE=Release
CMAKE_VERBOSE_MAKEFILE=OFF
build_deps=0
CACHELIB=$PWD/ext/CacheLib-Holpaca

die() { echo "$*" 1>&2 ; exit 1; }

show_help_and_exit() {
  base=$(basename "$0")
  echo "YCSB-cpp dependencies builder

usage: $base [-dhbjv] NAME

options:
  -b also build dependencies
        (default is to skip building dependencies)
  -p    set installation prefix
        (default is $PREFIX)
  -d    build with DEBUG configuration
        (default is RELEASE with debug information)
  -h    This help screen
  -j    build using all available CPUs ('make -j')
        (default is to use single CPU)
  -v    verbose build
"
  exit 0
}


while getopts ":j:p:bvdh" param; do
  case $param in
  p) PREFIX=$OPTARG ;;
  h) show_help_and_exit ;;
  j) JOBS=$OPTARG ;;
  d) CMAKE_BUILD_TYPE=RelWithDebInfo ;;
  v) CMAKE_VERBOSE_MAKEFILE=ON ;;
  b) build_deps=1 ;;
  ?)
    echo "unknown option. See -h for help."
    exit 1
    ;;
  esac
done
shift $((OPTIND - 1))

# Environment variables
export CMAKE_PREFIX_PATH="$PREFIX/lib/cmake:$PREFIX/lib64/cmake:$PREFIX/lib:$PREFIX/lib64:$PREFIX:${CMAKE_PREFIX_PATH:-}"
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"
export LD_LIBRARY_PATH="$PREFIX/lib:$PREFIX/lib64:${LD_LIBRARY_PATH:-}"
export LIBRARY_PATH="$PREFIX/lib:$PREFIX/lib64:${LD_LIBRARY_PATH:-}"
export PATH="$PREFIX/bin:$PATH"

# Build settings
CMAKE_FLAGS=(
  "-DCMAKE_INSTALL_PREFIX=$PREFIX"
  "-DCMAKE_MODULE_PATH=$PWD/cmake/;$PREFIX/lib/cmake/;$CACHELIB/cachelib/cmake/"
  "-DBUILD_SHARED_LIBS=ON"
  "-DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE"
  "-DCMAKE_VERBOSE_MAKEFILE=$CMAKE_VERBOSE_MAKEFILE"
  "-DCMAKE_INSTALL_RPATH=$PREFIX/lib"
  "-DCMAKE_INSTALL_RPATH_USE_LINK_PATH=FALSE"
  "-DCMAKE_BUILD_WITH_INSTALL_RPATH=TRUE"
)

# <name> <branch/tag> <shallow?> <recurse-submodules?> <repository> <cmake-source-dir> [<cmake-flag>...]
dependencies=( 
  "zstd v1.5.6 yes no https://github.com/facebook/zstd build/cmake -DZSTD_BUILD_TESTS=OFF"
  "glog v0.5.0 yes no https://github.com/google/glog . -DWITH_GFLAGS=OFF"
  "gflags v2.2.2 yes no https://github.com/gflags/gflags . -DGFLAGS_BUILD_TESTING=NO"
  "googletest v1.15.2 yes no https://github.com/google/googletest ."
  "fmt 8.0.1 yes no https://github.com/fmtlib/fmt . -DFMT_TEST=NO"
  "sparsemap v0.6.2 yes no https://github.com/Tessil/sparse-map ."
  "folly v2022.09.19.00 yes yes https://github.com/facebook/folly . -DBUILD_TESTS=OFF"
  "rocksdb main yes no https://github.com/vaavaav/rocksdb . -DWITH_TESTS=OFF -DWITH_GFLAGS=OFF -DWITH_BENCHMARK_TESTS=OFF"
  "fizz v2022.09.19.00 yes yes https://github.com/facebookincubator/fizz fizz -DBUILD_TESTS=OFF"
  "wangle v2022.09.19.00 yes yes https://github.com/facebook/wangle wangle -DBUILD_TESTS=OFF"
  "fbthrift v2022.09.19.00 yes yes https://github.com/facebook/fbthrift . -DCMAKE_BUILD_WITH_INSTALL_RPATH=FALSE"
  "grpc v1.50.1 yes yes https://github.com/grpc/grpc . -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_ZLIB_PROVIDER=package -DgRPC_SSL_PROVIDER=package -DABSL_PROPAGATE_CXX_STD=ON -Dprotobuf_WITH_ZLIB=ON -DCMAKE_BUILD_WITH_INSTALL_RPATH=FALSE"
  "flows master no no https://gitfront.io/r/vaavaav/davMvwJ8jxJv/flows-cpp.git ."
  "CacheLib-Holpaca dev-new yes no https://github.com/vaavaav/CacheLib-Holpaca cachelib -DBUILD_TESTS=OFF -DCMAKE_FIND_DEBUG_MODE=ON" 
)

# Installing dependencies
if [ $build_deps -eq 1 ]; then
  mkdir -p ext
  # Clone dependencies
  for d in "${dependencies[@]}"
  do
    set -- $d
    if [ ! -d "ext/$1" ]; then
      GIT_FLAGS="-b $2"
      if [ "$3" == "yes" ]; then
        GIT_FLAGS="$GIT_FLAGS --depth 1"
      fi
      if [ "$4" == "yes" ]; then
        GIT_FLAGS="$GIT_FLAGS --recurse-submodules --shallow-submodules"
      fi
      git clone $GIT_FLAGS "$5" "ext/$1" || (rm -r "ext/$1" ; die "failed to clone $1")
    fi
  done
  # Build dependencies
  for d in "${dependencies[@]}"
  do
    set -- $d
    cmake "${CMAKE_FLAGS[@]}" "${@:7}" -B"build-$1" -S"ext/$1/$6" \
    && make -C"build-$1" -j"$JOBS" install \
    || die "failed to build $1" 
  done
fi

cmake "${CMAKE_FLAGS[@]}" -B"build-ycsb" \
  && make -C"build-ycsb" -j$JOBS install \
  || die "failed to build ycsb" 
  
