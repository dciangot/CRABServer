"""
Implement logic:
if [[ $DEST_LFN =~ ^/store/user/rucio/* ]]; then
        timeout 15m python task_process/RUCIO_Transfers.py
        else
        timeout 15m python task_process/FTS_Transfers.py
        fi

using Transfers class definitions
"""