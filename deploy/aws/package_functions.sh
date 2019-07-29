#!/bin/bash
set -ueo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
TMP_DIR=$(mktemp -d)

PROJECT="wx_explore"
FUNCTIONS_DIR="${PROJECT}/cloud/functions"
OUT_DIR="${REPO_DIR}/deploy"

pushd "${TMP_DIR}"

# main code and requirements
cp -r "${REPO_DIR}/${PROJECT}" .
cp -r "${REPO_DIR}/requirements.functions.txt" "requirements.txt"

# TODO: cleanup dotfiles

# Ubuntu requires the --system because dumb
pip3 install -r requirements.txt --system --target .

for func_file in ${FUNCTIONS_DIR}/*.py; do
    func_filename=$(basename "${func_file}")
    func_name="${func_filename%.*}"
    cp "${func_file}" func.py
    zip -q -r "${OUT_DIR}/aws-${func_name}-$(date '+%Y-%m-%d-%H-%M-%S').zip" .
    rm func.py
done

popd

rm -rf "${TMP_DIR}"
