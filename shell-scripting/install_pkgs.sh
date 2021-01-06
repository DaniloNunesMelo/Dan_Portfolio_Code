#!/bin/bash
#Author: Danilo Melo
#Installing mutliple pakages

if [[ $# -eq 0 ]]
then
  echo "Usage: Please, enter the packages separeted by space: $0 pkg1 pkg2 ..."
  exit 1 #Exit with no success 
fi


if [[ $(id -u) -ne 0 ]]
then
  echo "Please, run from root user or with sudo privilage"
  exit 2 #Exit with no success
fi


for each_pkg in $@ #taking all arguments for loop
do
  if which $each_pkg &> /dev/null
  then
     echo "Already $each_pkg is installed"
  else
     echo "Installing $each_pkg ..."
     apt-get install $each_pkg -y &> /dev/null #discarting msg to not show on scream
     if [[ $? -eq 0 ]]
     then
       echo "Successfully installed $each_pkg pkg"
     else
       echo "Unable to install vim $each_pkg"
     fi
  fi

done