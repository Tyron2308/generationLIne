# Ingestion Multi Source

Source du projet pour l'ingestion en Spark Scala


## Contenu du projet

```bash
.
├── project
│   └── build.properties
├── src
│   ├── main/scala/lbp/ingestion
│   │   ├── Ingestion.scala
│   │   ├── IngestionMode.scala
│   │   ├── IngestionSchema.scala
│   │   ├── IngestionStructure.scala
│   │   └── Main.scala
│   └── test
│       ├── resources/ingress
│       │     └── m72005aa.aflaall.2018071916393216
│       ├── resources/metadata
│       │     └── m72005aa.aflaall.schema
│       └── scala/lbp/ingestion
│             └── IngestionTest.scala
├── .gitignore
├── .gitlab-ci.yml
├── build.sbt
└── README.md
```


## Objectif du projet

### Ingestion

Comme dit en introduction, l'objectif est de refaire un package d'ingestion des données en Spark Scala afin de pouvoir confronter les performances de Spark et Nifi.

Pour ce faire, le job ira récupérer tous les fichiers dans un répertoire source (en l'occurence */coffre_fort/ingress/copyRawFiles*) et dont le nom de fichier commence par un préfix donné en paramètre.


L'ingestion des données se fera de la manière suivante :

1. Listing de tous les fichier commençant par le préfixe.
2. Récupération des schémas associés à ces fichiers (stockés */coffre_fort/administration/\*/metadata/\<source\>/\** ).
3. Suppression des anciennes données ( Full / Delete ) : les données brutes ainsi que les tables Hive.
4. Validation des lignes du fichier et écriture des lignes invalides sur HDFS
5. Exposition des données dans Hive et écriture dans HDFS ( */coffre_fort/datalake/\*/an0/\<source\>/\<tableName\>* )
6. Application des règles de transformation (anonymisation)
7. Supression du fichier depuis la zone de reprise */coffre_fort/ingress/copyRawFiles*.


### Anonymisation

Pour l'ingestion des données, une demande du métier a été faite afin d'anonymiser les données. Lors de l'ingestion, les données avaient été stockées dans ***an0*** (qui représente le niveau d'anonymisation le plus faible). Mais à l'heure actuelle, il y a un niveau d'anonymisation plus élevé qui est ***an1***.

Pour procéder à cette anonymisation, une fois que les données sont stockées dans Hive, on exécute une requête SQL afin d'annuler certains champs. Par exemple, on est amené à exécuter la requête suivante :

```sql
SELECT,
	id_ttl,
	cd_typ_ttl,
	cd_nat_cnt,
	cd_typ_cnt,
	no_cnt_det,
	dt_deb_val,
	cd_prp_cnt,
	cd_dev,
	id_elt_str_dem,
	id_elt_str_ele,
	dt_dmd_int_bdf,
	dt_eta_cnt,
	dt_fin_val,
	cd_eta_cnt,
	id_elt_str,
	cd_cnt_typ,
	num_cnt_com,
	cd_fam_cnt,
	null as lb1_int_spe,
	null as lb2_int_spe,
	dt_ouv_elt_cnt_det,
	dt_fer_elt_cnt_det,
	lb_min_dev,
	ind_inclus_det,
	par_prd_spp_idq,
	dt_maj_sid,
FROM ddup_an0.te_elt_cnt_det_ssp
```

Une fois les données récupérées, elles seront sauvegardées dans */coffre_fort/datalake/la_banque_postale/an1/\<source\>/\<tableName\>* et dans la table Hive correspondante.


### Monitoring

L'application mesure et conserve dans une bases dédiées les informations suivantes :

- **filename** : nom du fichier ingéré
- **total_lines** : nombre de ligne du fichier
- **valid_number** : nombre de ligne valide du fichier
- **invalid_number** : nombre de ligne invalide du fichier


Exemple de résultat dans Hive :

| filename                          | total_lines | valid_number | invalid_number |
|-|-|-|-|
| m72047aa.aflaall.2018071916393216 | 83622        | 83622        | 0              |
| m72027aa.aflaall.2018071916393216 | 13371        | 13371        | 0              |
| m72005aa.aflaall.2018071916393216 | 10729        | 10729        | 0              |
| m72004aa.aflaall.2018071916393216 | 10729        | 10729        | 0              |



#### Les logs

Afin de pouvoir surperviser l'ingestion, un fichier le log sera créé dans le dossier suivant : */home/\<user\>/ingestion/ingestion.log*.

**Attention !! Il s'agira du $HOME du user qui lance le job**

Nous avons essayé de mettre en place des logs plutôt exhaustifs afin de pouvoir plus facilement debugger en cas de problème.

Exemple de résultat dans les logs :

```
2018-12-05 11:03:36,273 - INFO - ==================================================
2018-12-05 11:03:36,274 - INFO - ==== STARTING INGESTION: 05/12/2018 11:03:36 =====
2018-12-05 11:03:36,274 - INFO - ==================================================
2018-12-05 11:03:59,284 - INFO - Starting ingestion for file: m72001aa.aflaall.2018071916393216
2018-12-05 11:04:06,667 - INFO - Delete old ingestion for file: m72001aa.aflaall.2018071916393216
2018-12-05 11:04:06,688 - INFO - Truncate tables: TA_PRS__TTL
2018-12-05 11:04:07,310 - INFO - Save row file into /coffre_fort/datalake/la_banque_postale/raw/ddup/TA_PRS__TTL/m72001aa.aflaall.2018071916393216
2018-12-05 11:04:48,507 - INFO - Total lines in m72001aa.aflaall.2018071916393216: 21931000
2018-12-05 11:05:26,495 - INFO - Total lines valid: 21931000
2018-12-05 11:05:26,495 - INFO - Total lines invalid: 0
2018-12-05 11:05:27,892 - INFO - Write valid lines into /coffre_fort/datalake/la_banque_postale/an0/ddup/TA_PRS__TTL/m72001aa.aflaall.2018071916393216
2018-12-05 11:06:01,921 - INFO - Insert valid lines into ddup_an0.ta_prs__ttl
2018-12-05 11:06:47,501 - INFO - Anonymization of data
2018-12-05 11:06:48,336 - INFO - Write anonymous lines into /coffre_fort/datalake/la_banque_postale/an1/ddup/TA_PRS__TTL/m72001aa.aflaall.2018071916393216
2018-12-05 11:06:48,336 - INFO - 12345678
2018-12-05 11:07:08,807 - INFO - Insert valid lines into ddup_an1.ta_prs__ttl
2018-12-05 11:07:44,484 - INFO - End of valid data for: m72001aa.aflaall.2018071916393216

...

2018-12-05 11:11:25,706 - INFO - Starting ingestion for file: m72047aa.aflaall.2018071916393216
2018-12-05 11:11:25,866 - INFO - Delete old ingestion for file: m72047aa.aflaall.2018071916393216
2018-12-05 11:11:25,871 - INFO - Truncate tables: VA_SEG_MKT_CLI_MOD_SRV_01
2018-12-05 11:11:26,163 - INFO - Save row file into /coffre_fort/datalake/la_banque_postale/raw/ddup/VA_SEG_MKT_CLI_MOD_SRV_01/m72047aa.aflaall.2018071916393216
2018-12-05 11:11:26,513 - INFO - Total lines in m72047aa.aflaall.2018071916393216: 83622
2018-12-05 11:11:26,735 - INFO - Total lines valid: 83622
2018-12-05 11:11:26,735 - INFO - Total lines invalid: 0
2018-12-05 11:11:26,756 - INFO - Write valid lines into /coffre_fort/datalake/la_banque_postale/an0/ddup/VA_SEG_MKT_CLI_MOD_SRV_01/m72047aa.aflaall.2018071916393216
2018-12-05 11:11:27,029 - INFO - Insert valid lines into ddup_an0.va_seg_mkt_cli_mod_srv_01
2018-12-05 11:11:27,562 - INFO - Anonymization of data
2018-12-05 11:11:27,703 - INFO - Write anonymous lines into /coffre_fort/datalake/la_banque_postale/an1/ddup/VA_SEG_MKT_CLI_MOD_SRV_01/m72047aa.aflaall.2018071916393216
2018-12-05 11:11:27,703 - INFO - 12345678
2018-12-05 11:11:27,963 - INFO - Insert valid lines into ddup_an1.va_seg_mkt_cli_mod_srv_01
2018-12-05 11:11:28,527 - INFO - End of valid data for: m72047aa.aflaall.2018071916393216
2018-12-05 11:11:28,733 - INFO - Save monitoring data
2018-12-05 11:11:29,188 - INFO - Remove file from source directory: m72047aa.aflaall.2018071916393216
2018-12-05 11:11:29,193 - INFO - End of process for: m72047aa.aflaall.2018071916393216
2018-12-05 11:11:29,193 - INFO - ==================================================
2018-12-05 11:11:29,193 - INFO - ================ END OF INGESTION ================
2018-12-05 11:11:29,193 - INFO - ==================================================

```



## Les prérequis

Pour le bon déroulement du job, les prérequis suivants sont nécessaires :

- Une base de données **\<source\>_an0** ainsi que les tables doivent être présentes sur Hive.
- Une base de données **\<source\>_an1** (si le niveau d'anonymisation est de 1) ainsi que les tables doivent être présentes sur Hive (il s'agit des mêmes tables que dans *an1*).
- Une base de données **monitoring** ainsi qu'une table **<source>**. Le DDL est le suivant :

  ```
  CREATE DATABASE IF NOT EXISTS monitoring;

  CREATE TABLE monitoring.<source> (
    filename string,
    total_lines int,
    valid_number int,
    invalid_number int
  );
  ```


## Le lancement

Pour lancer le job, on lance la commande suivante :

```shell
$ spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.executor.instances=7 \
    --class lbp.ingestion.Main \
    ingestion_2.11-0.2.jar \
    "<appName>" \
    <sourcesDirectory> \
    <prefixFile> \
    <datalakeDirectory> \
    <levelAnonymization> \
    <ingestionMode = (FULL_TOTAL|FULL_HIST|APPEND)>
```


## Les tests

Afin de tester certaines fonctions du package, des tests unitaires ont été mis en place. Le lancement des tests se fait avec **sbt**. Pour cela, il suffira de lancer la commande suivante :

```shell
$ sbt test
```