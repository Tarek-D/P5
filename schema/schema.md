# Schéma de la collection healthcare

La collection stocke des données médicales extraites du fichier CSV `healthcare_dataset.csv`.

## Champs attendus

| Champ                 | Type    | Description                         | Obligatoire |
|-----------------------|---------|-----------------------------------|-------------|
| Name                  | string  | Nom du patient                    | Oui         |
| Age                   | int     | Âge du patient                   | Oui         |
| Gender                | string  | Sexe (Male, Female, etc.)        | Oui         |
| Blood Type            | string  | Groupe sanguin                   | Non         |
| Medical Condition     | string  | Description de la maladie         | Oui         |
| Date of Admission     | date    | Date d’entrée à l’hôpital         | Oui         |
| Doctor                | string  | Nom du médecin responsable       | Oui         |
| Hospital              | string  | Nom de l’établissement hospitalier | Oui      |
| Insurance Provider    | string  | Compagnie d’assurance             | Non         |
| Billing Amount        | decimal | Montant facturé                  | Non         |

## Index

- Index unique sur `Name` + `Date of Admission`.
- Index secondaire sur `Medical Condition`.

---

Les types sont ceux attendus dans MongoDB (ex: string, int, decimal, date).  
Les documents doivent respecter cette structure pour assurer la cohérence des traitements futurs.
