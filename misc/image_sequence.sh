#!/bin/bash
set -ueo pipefail

if [ $# -lt 3 ]; then
    echo "Usage: $0 'SELECTOR' 'COLOR MAP' 'OUT FILE' ['START HOUR' 'END HOUR']"
    echo "Example: $0 'CRAIN/0 - NONE/20200618/0400' '0,0,0,0;1,255,255,255' \$PWD/test.mp4"
    exit 1
fi

SELECTOR=$1
CM=$2
OUT_FILE=$3
START_HOUR=${4:-0}
END_HOUR=${5:-18}

TMP_DIR=$(mktemp -d)

pushd "${TMP_DIR}"

for t in $(seq -f "%03g" "$START_HOUR" "$END_HOUR"); do
    echo "https://rio7uowgl3.execute-api.us-east-1.amazonaws.com/?s3_path=http://noaa-hrrr-pds.s3.us-east-1.amazonaws.com/${SELECTOR}/${t}&cm=${CM}";
done | parallel --will-cite -j $((END_HOUR - START_HOUR)) wget --quiet -O '{#}.png' '{}'

ffmpeg -nostdin -v quiet -r 5 -i %d.png -c:v libx264 -vf fps=25 -vf "pad=ceil(iw/2)*2:ceil(ih/2)*2" -pix_fmt yuv420p "${OUT_FILE}"

popd

rm -rf "${TMP_DIR}"
