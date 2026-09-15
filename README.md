# market-observatory

Collecte mutualisee de donnees de marche et archives incrementales chiffrees.
Aucune barre ni aucun secret dans Git ; seuls les referentiels de symboles y figurent.
Etat : validation locale, pas de service
deploye. Le workflow est exclusivement manuel et execute des tests hors reseau.
Aucun cron, aucune collecte Actions, aucune publication automatique.

## Validation

Dans un environnement virtuel Python, depuis ce dossier :

```sh
python -m pip install -r requirements-backfill.txt
python -B -m unittest discover -s tests -v
```

## Collecte Manuelle

Le registre SQLite conserve les fenetres observees et les erreurs. Les appels
sont bornes a 30 concurrents ; les tentatives respectent le cooldown de source.
Une reponse vide ou erronee ne devient pas une fenetre complete. Les mappings
d'instruments non traduits sont exclus avec un rapport explicite. L'univers
`universes/global-20260915.csv` contient 64 986 symboles mappes ; son inventaire
conserve 1 221 exclusions non resolues et 17 alias dedupliques.

```sh
python scripts/incremental_collector.py --config config/manual.yaml --store state/collector \
  collect --universe /absolute/path/validated-universe.csv \
  --start 2026-09-14T12:00:00Z --end 2026-09-14T13:00:00Z --interval 1m
```

Cette commande planifie seulement. `--execute` autorise les appels reseau de ce
passage manuel. Relancer reprend les fenetres en attente ; une revision explicite
cree une observation distincte sans ecraser la precedente.

Les recus natifs fermes s'importent avant la planification avec
`import-observations --inventory /absolute/path/inventory.json`. Les bases actives
ne doivent pas etre copiees. Les autres formats exigent un adaptateur explicite.

`manual_pipeline.py` relie checkpoint, import des recus compatibles, planification
bornee, collecte, export ferme, preparation et publication optionnelle. Sans
`--execute`, il ne collecte rien. `--publish` est une autorisation distincte.
`--parent` designe un recu epingle ; `--bootstrap` est reserve au premier lot.
Utiliser un nouveau repertoire de travail pour chaque nouveau parent. Les trous
contigus sont regroupes en requetes bornees et les exports producteur en lots
JSONL compresses par intervalle, sans fichier archive par symbole/heure.

## Archives

`scripts/object_archive.py` fournit `prepare`, `restore` et `compact` en local :
deduplication SHA-256, packs zstd/age de 8 Mio par defaut, catalogues chiffres
epingles, restauration selective et recus verifies. La compaction est une
simulation sans `--execute`. Elle ne supprime jamais les anciens objets/pins.
Les clients reutilisent leurs blocs meme apres changement de packs physiques.

La cle est lue depuis le chemin configure, avec permissions owner-only requises.
Elle n'est ni modifiee ni distribuee par ce projet. `release_transport.py` fournit
`publish`, `resolve`, `restore`, `lock-status` et la recuperation manuelle `unlock`.
Le depot destinataire doit etre prive, initialise et ses releases immuables activees.
Les objets sont publies avant READY ; les clients ignorent les lots incomplets.
Le checkpoint chiffre suffit a reprendre sur un runner neuf sans tout restaurer.
Ces chemins passent les tests offline ; leur publication GitHub reelle reste a valider.
L'ancien script de publication non chiffree est desactive.

`backfill_sec.py` exporte des tables existantes en lecture seule vers Parquet/Zstd.
`backfill_parquet.py` reutilise des exports dates via rsync et controle leurs hashes.
Ni base active, ni WAL, ni modification de permissions du serveur.

Un snapshot prouve l'identite de ses octets, pas une couverture PIT complete.
Les acquisitions courantes restent classees `provider_current_non_pit` ; les
compositions d'indices, corporate actions et revisions ont leurs propres contrats.
`pit_records.py` selectionne les revisions selon connaissance et validite, y compris
corrections et retraits. Il ne fabrique pas les dates manquantes.
`config/datasets.yaml` distingue les adaptateurs presents des collecteurs et
historiques encore requis : leur inventaire ne signifie pas leur acquisition.
