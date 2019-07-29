#!/bin/bash
set -ueo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
TMP_DIR=$(mktemp -d)

PROJECT="wx_explore"
FUNCTIONS_DIR="${PROJECT}/cloud/functions"
OUT_DIR="${REPO_DIR}/deploy"

pushd "${TMP_DIR}"

# azure specific config stuff
cp "${SCRIPT_DIR}/host.json" .
cp "${SCRIPT_DIR}/local.settings.json" .

# main code and requirements
cp -r "${REPO_DIR}/${PROJECT}" .
cp -r "${REPO_DIR}/requirements.functions.txt" "requirements.txt"

# TODO: cleanup dotfiles

for func_file in ${FUNCTIONS_DIR}/*.py; do
    func_filename=$(basename "${func_file}")
    func_name="${func_filename%.*}"
    # Create folder for each function
    mkdir "${func_name}"
    # and link in function code and stock config json
    cp "${func_file}" "${func_name}/__init__.py"
    cp "${SCRIPT_DIR}/function.json" "${func_name}/"
done

func pack --build-native-deps

mv tmp.*.zip "${OUT_DIR}/azure-$(date '+%Y-%m-%d-%H-%M-%S').zip"

popd

rm -rf "${TMP_DIR}"
