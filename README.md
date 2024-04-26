# Template pour un projet scala + spark + sbt

## Introduction

Ce template a été créé en utilisant la version 11 de java.

## Prérequis

Pour l'utiliser, assurez vous d'avoir `java11` installé et la variable d'environnement `JAVA_HOME` définie vers le dossier ou java11 est présent.

Il faudra aussi avoir sbt d'installer.

## Utilisation

Pour lancer le projet utiliser la commande:

```bash
sbt run -java-home $JAVA_HOME
```

Ce programme Scala utilise Apache Spark pour effectuer diverses analyses de texte, telles que le comptage de mots par ligne et la détermination de la moyenne et de la
médiane des mots par ligne dans un ensemble de données de livres.

![](screens/booksNormal.png "Optional title")

On extrait les données des livres depuis un fichier .txt pour ensuite les nettoyer et les transformer dans des Dataframes lesquels on exploite pour produire les données.

Fonctionnalités : 
- Identification des nouveaux livres à partir des lignes contenant "ISBN".
- Agrégation du contenu par livre.
- Calcul du nombre de mots par ligne et par livre.
- Calcul de la moyenne et de la médiane des mots par ligne dans l'ensemble des livres.
- Création des visuels pour observer ces statistiques.

![](screens/booksStats.png "Optional title")

## Avertissement
Assurez-vous d'avoir suffisamment de ressources disponibles sur votre machine pour exécuter les analyses, car Apache Spark peut consommer beaucoup de mémoire et de puissance de calcul.