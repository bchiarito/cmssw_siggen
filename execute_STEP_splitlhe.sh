#! /bin/bash
echo ">>> Starting job on" `date`
echo ">>> Running on: `uname -a`"
echo ">>> System software: `cat /etc/redhat-release`"
echo ""
echo "Here there are all the input arguments:"
echo $@
echo ""
python3 split_helper.py $1 $2 $3 -s=$4
