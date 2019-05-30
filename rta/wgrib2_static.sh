#!/bin/bash
set -ue

SCRIPTDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TMPDIR=$(mktemp -d)

pushd "${TMPDIR}"

wget http://www.ftp.cpc.ncep.noaa.gov/wd51we/wgrib2/wgrib2.tgz
tar -xf wgrib2.tgz

cd grib2

# patch up makefile directly since LDFLAGS are clobbered
sed -i 's/{LDFLAGS}/{LDFLAGS} -lquadmath -lm -lmvec -static/' wgrib2/makefile

# build with flag to fix https://gcc.gnu.org/ml/gcc-help/2012-01/msg00183.html
FC=gfortran CC=gcc CFLAGS="-Wl,-upthread_mutex_destroy" make -j4

cp wgrib2/wgrib2 "${SCRIPTDIR}"

popd

rm -rf "${TMPDIR}"
