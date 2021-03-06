  
FROM ubuntu:18.04

LABEL author="xiaobo <peterwillcn@gmail.com>" maintainer="Xiaobo <peterwillcn@gmail.com> Huang-Ming Huang <huangh@objectcomputing.com> Winlin <pcguangtao@gmail.com>" version="0.1.2" \
  description="This is a base image for building boscore/bos"

RUN echo 'APT::Install-Recommends 0;' >> /etc/apt/apt.conf.d/01norecommends \
  && echo 'APT::Install-Suggests 0;' >> /etc/apt/apt.conf.d/01norecommends \
  && apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y sudo wget curl net-tools ca-certificates unzip gnupg

RUN echo "deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-9 main" >> /etc/apt/sources.list.d/llvm.list \
  && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key|sudo apt-key add - \
  && apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y git-core automake autoconf libtool build-essential pkg-config libtool \
     mpi-default-dev libicu-dev python-dev python3-dev libbz2-dev zlib1g-dev libssl-dev libgmp-dev doxygen graphviz libgmp3-dev \
     ninja-build libusb-1.0-0-dev curl libcurl4-gnutls-dev autotools-dev ruby \
     clang-9 lldb-9 lld-9 libllvm-9-ocaml-dev libllvm9 llvm-9 llvm-9-dev llvm-9-doc llvm-9-examples llvm-9-runtime clang-9 clang-tools-9 \
     clang-9-doc libclang-common-9-dev libclang-9-dev libclang1-9 clang-format-9 python-clang-9 clangd-9 libfuzzer-9-dev lldb-9 lld-9 \
     libc++-9-dev libc++abi-9-dev libomp-9-dev \
  && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/clang clang /usr/lib/llvm-9/bin/clang 900 \
  && update-alternatives --install /usr/bin/clang++ clang++ /usr/lib/llvm-9/bin/clang++ 900

RUN wget https://cmake.org/files/v3.9/cmake-3.9.6-Linux-x86_64.sh \
    && bash cmake-3.9.6-Linux-x86_64.sh --prefix=/usr/local --exclude-subdir --skip-license \
    && rm cmake-3.9.6-Linux-x86_64.sh

ENV CC clang
ENV CXX clang++

RUN wget https://dl.bintray.com/boostorg/release/1.71.0/source/boost_1_71_0.tar.bz2 -O - | tar -xj \
    && cd boost_1_71_0 \
    && ./bootstrap.sh --prefix=/usr/local \
    && echo 'using clang : 9 : clang++-9 ;' >> project-config.jam \
    && ./b2 -d0 -j$(nproc) --with-thread --with-date_time --with-system --with-filesystem --with-program_options \
       --with-serialization --with-chrono --with-test --with-context --with-locale --with-coroutine --with-iostreams toolset=clang link=static install \
    && cd .. && rm -rf boost_1_71_0

RUN wget https://github.com/mongodb/mongo-c-driver/releases/download/1.10.2/mongo-c-driver-1.10.2.tar.gz -O - | tar -xz \
    && cd mongo-c-driver-1.10.2 \
    && mkdir cmake-build && cd cmake-build \
    && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local -DENABLE_BSON=ON \
		-DENABLE_SSL=OPENSSL -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF -DENABLE_STATIC=ON .. \
    && make -j$(nproc) \
    && make install \
    && cd ../../ && rm -rf mongo-c-driver-1.10.2

# RUN git clone --depth 1 --single-branch --branch release_90 https://github.com/llvm-mirror/llvm.git \
#     && git clone --depth 1 --single-branch --branch release_90 https://github.com/llvm-mirror/clang.git llvm/tools/clang \
#     && cd llvm \
#     && cmake -H. -Bbuild -GNinja -DCMAKE_INSTALL_PREFIX=/opt/wasm -DLLVM_TARGETS_TO_BUILD= -DLLVM_EXPERIMENTAL_TARGETS_TO_BUILD=WebAssembly -DCMAKE_BUILD_TYPE=Release  \
#     && cmake --build build --target install \
#     && cd .. && rm -rf llvm

RUN git clone --depth 1 -b releases/v3.3 https://github.com/mongodb/mongo-cxx-driver \
    && cd mongo-cxx-driver/build \
    && cmake -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local .. \
    && make -j$(nproc) \
    && make install \
    && cd ../../ && rm -rf mongo-cxx-driver

RUN git clone --depth 1 -b v0.11.6 https://github.com/boscore/librdkafka.git \
    && cd librdkafka/ \
    && cmake -H. -B_cmake_build \
    && cmake -DRDKAFKA_BUILD_STATIC=1 --build _cmake_build \
    && cd _cmake_build \
    && make install \
    && cd ../../ && rm -rf librdkafka

RUN git clone --depth 1 -b 0.2 https://github.com/boscore/cppkafka.git \
    && cd cppkafka/ \
    && mkdir build && cd build \
    && cmake -DCPPKAFKA_RDKAFKA_STATIC_LIB=1 -DCPPKAFKA_BUILD_SHARED=0 .. \
    && make install \
    && cd ../../ && rm -rf cppkafka