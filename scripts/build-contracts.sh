#!/usr/bin/env bash
# Кладёт сборку реестра контрактов в папку contracts/ репозитория (она в .gitignore: dll компании в публичный репозиторий не попадают).
#
#   scripts/build-contracts.sh <папка с исходниками event-registry>   # собрать из исходников (git не трогается)
#   scripts/build-contracts.sh --from <папка с готовой сборкой>         # просто скопировать уже собранное, например ~/replay-contracts
#
# После этого: --contracts contracts/Dodo.EventRegistry.dll
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
out="$repo/contracts"

if [[ $# -eq 2 && "$1" == "--from" ]]; then
  src="$(cd "$2" && pwd)"
  [[ -f "$src/Dodo.EventRegistry.dll" ]] || { echo "в $src нет Dodo.EventRegistry.dll" >&2; exit 1; }
  find "$out" -mindepth 1 ! -name README.md -delete
  cp -r "$src"/. "$out"/
elif [[ $# -eq 1 ]]; then
  registry="$(cd "$1" && pwd)"
  project="$registry/src/Dodo.EventRegistry"
  [[ -d "$project" ]] || { echo "нет папки $project — укажи корень event-registry" >&2; exit 1; }
  find "$out" -mindepth 1 ! -name README.md -delete
  dotnet build "$project" -c Release -o "$out" -p:CopyLocalLockFileAssemblies=true
else
  sed -n '2,7p' "$0" >&2
  exit 2
fi

echo
echo "контракты: $out/Dodo.EventRegistry.dll ($(ls "$out" | wc -l) файлов)"
echo "запуск:    --contracts contracts/Dodo.EventRegistry.dll"
