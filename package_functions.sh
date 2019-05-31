#!/bin/bash
set -ueo pipefail

PROJECT="wx_explore"
FUNCTIONSDIR="${PROJECT}/cloud/functions"
OUTDIR="deploy"

SCRIPTDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TMPDIR=$(mktemp -d)

mkdir -p "${SCRIPTDIR}/${OUTDIR}"
rm -f "${SCRIPTDIR}/${OUTDIR}/*"

pushd "${TMPDIR}"
cp -r "${SCRIPTDIR}/requirements.openwhisk.txt" .
cp -r "${SCRIPTDIR}/${PROJECT}" .

pip3 install -t . -r requirements.openwhisk.txt

# Already included
rm -rf numpy*

mkdir lib

libs="libeccodes.so.0 libpng16.so.16 libaec.so.0 libopenjp2.so.7 libeccodes_memfs.so.0"
for lib in $libs; do
    cp -L "/usr/lib/x86_64-linux-gnu/${lib}" lib/
done

for func in ${FUNCTIONSDIR}/*.py; do
    func_file=$(basename "${func}")
    func_name="${func_file%.*}"
    echo "${func_name}"
    ln -s "${func}" __main__.py
    echo -e "import os;os.system('cp -r lib/* /usr/lib/x86_64-linux-gnu/')\n$(cat __main__.py)" > __main__.py
    zip -rq --exclude=\*__pycache__\* "${SCRIPTDIR}/${OUTDIR}/${func_name}.zip" .
    rm -f __main__.py
done

popd

echo "${TMPDIR}"
#rm -rf "${TMPDIR}"
