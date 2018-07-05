# MISINFORMATION ANALYSIS

Un piccolo progetto *SPARK* e *SPARKSQL* per fare delle analisi sulla diffusione della *MISINFORMATION* in un contesto distribuito su dati estrapolati da *Twitter* e salvati in *MongoDB*.
Ogni task è formato da una applicazione avviabile separatamente utilizzando *GRADLE*.

### HOW TO LAUNCH

* ANALISI 0

Quantità di Hashtag e Menzioni trovate

```zsh
> gradle fatJar0
> $SPARK_HOME/bin/spark-submit --class spark.sql.HashtagMentionSQL ~/Development/JAVA/MisinformationAnalysis/build/libs/HashtagMentionSQL-all-1.0.0.jar
```

* ANALISI 1

*Polarità* degli utenti con le relative percentuali e *quantità di tweet* per ognuno di essi +
Identificazione di utenti fonte di misinformation o information in funzione del *rapporto friends/followers*
(valori bassi rispetto alla media indicano probabili fonti < soglia 1.0)

```zsh
> gradle fatJar1s
> $SPARK_HOME/bin/spark-submit --class spark.analysis.Polarity ~/Development/JAVA/MisinformationAnalysis/build/libs/Polarity-all-1.0.0.jar
```

```zsh
> gradle fatJar1s
> $SPARK_HOME/bin/spark-submit --class spark.analysis.PolaritySQL ~/Development/JAVA/MisinformationAnalysis/build/libs/PolaritySQL-all-1.0.0.jar
```

* ANALISI 2

Quali e quanti utenti hanno condiviso contenuti provenienti da *fonti mainstream* o di *misinformation*

```zsh
> gradle fatJar2s
> $SPARK_HOME/bin/spark-submit --class spark.analysis.Fonts ~/Development/JAVA/MisinformationAnalysis/build/libs/Fonts-all-1.0.0.jar
```

```zsh
> gradle fatJar2
> $SPARK_HOME/bin/spark-submit --class spark.sql.FontsSQL ~/Development/JAVA/MisinformationAnalysis/build/libs/FontsSQL-all-1.0.0.jar
```


* ANALISI 3

Riuso degli stessi *hashtag/mention* per utente (utili per identificare eventuali social bot).
Filtro utenti di cui ho almeno 6 tweet
 
```zsh
> gradle fatJar3s
> $SPARK_HOME/bin/spark-submit --class spark.analysis.SocialBot ~/Development/JAVA/MisinformationAnalysis/build/libs/SocialBot-all-1.0.0.jar
```

```zsh
> gradle fatJar3
> $SPARK_HOME/bin/spark-submit --class spark.sql.SocialBotSQL ~/Development/JAVA/MisinformationAnalysis/build/libs/SocialBotSQL-all-1.0.0.jar
```

* ANALISI 4

Gruppi di utenti che hanno utilizzato gli stessi *hashtag* (utili ad identificare comunità di diffusione automatica di misinformation).
Filtro gruppi di cui ho almeno 6 hashtag
 
```zsh
> gradle fatJar4s
> $SPARK_HOME/bin/spark-submit --class spark.analysis.HashtagsGroup ~/Development/JAVA/MisinformationAnalysis/build/libs/HashtagsGroup-all-1.0.0.jar
```
  
* ANALISI 5

Gruppi di utenti che hanno utilizzato le stesse *mention* (utili ad identificare comunità di diffusione automatica di misinformation).
Filtro gruppi di cui ho almeno 6 hashtag
 
```zsh
> gradle fatJar5s
> $SPARK_HOME/bin/spark-submit --class spark.analysis.MentionsGroup ~/Development/JAVA/MisinformationAnalysis/build/libs/MentionsGroup-all-1.0.0.jar
```

* POST ANALISI 6

Communities di utenti che hanno utilizzato gli stessi hashtag

```zsh
> gradle fatJar6s
> $SPARK_HOME/bin/spark-submit --class spark.analysis.post_analysis.HashtagsCommunity ~/Development/JAVA/MisinformationAnalysis/build/libs/HashtagsCommunity-all-1.0.0.jar
```
   
* POST ANALISI 7

Communities di utenti che hanno utilizzato le stesse mentions

```zsh
> gradle fatJar7s
> $SPARK_HOME/bin/spark-submit --class spark.analysis.post_analysis.MentionsCommunity ~/Development/JAVA/MisinformationAnalysis/build/libs/MentionsCommunity-all-1.0.0.jar
```

* POST ANALISI 8

Communities di utenti che hanno utilizzato gli stessi hashtag e mention. Scelgo comunità con la stessa polarità
 
```zsh
> gradle fatJar8s
> $SPARK_HOME/bin/spark-submit --class spark.analysis.post_analysis.HMCommunities ~/Development/JAVA/MisinformationAnalysis/build/libs/HMCommunities-all-1.0.0.jar
```

```zsh
> gradle fatJar8
> $SPARK_HOME/bin/spark-submit --class spark.sql.CommunitiesSQL ~/Development/JAVA/MisinformationAnalysis/build/libs/CommunitiesSQL-all-1.0.0.jar
```
