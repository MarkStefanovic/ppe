#!/bin/bash

conda deactivate && conda env remove -n linux-tools --all

echo "Done."

read -rn1
