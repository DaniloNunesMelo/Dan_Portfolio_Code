#!/bin/bash

cnt=0
for((;;))
do
  echo ${cnt}
  R=$(($RANDOM%1000))
  Year=$(shuf -i 1990-2021 -n 1)
  FileDate=$(date +"%FT%H%M%S")
  echo -e \\n "\"ITA\",\"Italy\",\"B12\",\"Outflows of foreign population by nationality\",\"TOT\",\"Total\",\"TUR\",\"Turkey\",\"2018\",\"${Year}\",${R},," > Datasets/Streaming/Mig_Italy_Streaming_${FileDate}.csv
  echo -e "\"ITA\",\"Italy\",\"B12\",\"Outflows of foreign population by nationality\",\"TOT\",\"Total\",\"TUR\",\"Turkey\",\"2018\",\"${Year}\",${R},,"
  ((cnt++))
  sleep 3
  if [[ $cnt -eq 000 ]]
  then
    break
  fi
done