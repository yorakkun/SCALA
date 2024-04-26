# Prérequis

Pour l'utiliser, assurez vous d'avoir `java11` installé et la variable d'environnement `JAVA_HOME` définie vers le dossier ou java11 est présent.

Il faudra aussi avoir sbt d'installer.

## Utilisation

Pour lancer le projet utiliser la commande:

```bash
sbt run -java-home $JAVA_HOME
```

## Introduction

Pourquoi avoir choisi Spark Scala pour ce projet :

1. **Traitement de données massives** : Apache Spark est spécialement conçu pour le traitement efficace de grands ensembles de données répartis sur un cluster. Étant donné que nous travaillons avec des données de livres qui pourraient potentiellement être volumineuses, Spark offre des capacités de traitement parallèle qui permettent de traiter ces données de manière rapide et efficace.

2. **Traitement distribué** : Scala est bien adapté pour exploiter les fonctionnalités de Spark, notamment son API RDD (Resilient Distributed Dataset) qui permet de manipuler des données distribuées de manière transparente. Cela nous permet d'exploiter les capacités de calcul distribué de Spark pour des analyses efficaces, même sur de grandes quantités de données.

En résumé, Spark Scala est un choix approprié pour ce projet en raison de sa capacité à gérer efficacement de grandes quantités de données distribuée et sa performance globale.


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