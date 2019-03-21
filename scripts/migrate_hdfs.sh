#!/bin/bash


WORKDIR="/Users/tyron/Desktop/Work/Keyrus/generateur_ligne/src/main/ressource/"

first_schema=$WORKDIR"/schema"
snd_schema=$WORKDIR"/test2.schema"

hdfs dfs -mkdir -p "/coffre_fort/ingress/copyRawFiles "
hdfs dfs -mkdir -p "/coffre_fort/datalake/administration/la_banque_postale/metadata/unica/"
hdfs dfs -mkdir -p "/coffre_fort/datalake/administration/la_banque_postale/metadata/segMkt/"

hdfs dfs -put  $first_schema "/coffre_fort/datalake/administration/la_banque_postale/metadata/segMkt/" 
hdfs dfs -put  $snd_schema "/coffre_fort/datalake/administration/la_banque_postale/metadata/unica/" 


hdfs dfs -ls -R /




