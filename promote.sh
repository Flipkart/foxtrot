#!/bin/bash
case $# in
    0 )
        echo "Usage: promote.sh stagech/nm"
        exit;;
    * )
        :;;
esac
ENV=$1
git tag -m "Promoting to $ENV" release/${ENV}-$(date +%Y%m%d-%H%M)
git push origin --tags
