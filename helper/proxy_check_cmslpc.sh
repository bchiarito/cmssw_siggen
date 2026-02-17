#! /bin/bash
path=$(voms-proxy-info --path)
if [[ -f "$path" ]]; then
  timeleft=$(voms-proxy-info --timeleft)
  if [[ $timeleft -eq 0 ]]; then
    voms-proxy-init --valid 168:00 -voms cms
  fi
  exit 0
else
  voms-proxy-init --valid 168:00 -voms cms
fi
