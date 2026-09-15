# market-observatory

Collecte mutualisee de donnees de marche et archives incrementales chiffrees.
Aucune barre ni aucun secret dans Git ; seuls les referentiels de symboles y figurent.
Etat : code publie, transport prive teste sur de vraies releases immuables.
Le workflow est exclusivement manuel et execute des tests hors reseau.
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

Le profil global planifie tous ces symboles en daily, 1m, 15m et 1h. L'intraday
commence au **15 septembre 2026 a 00:00, Europe/Paris**, sans backfill anterieur.
Cette ancre reste fixe : reutiliser le meme `--work` et avancer `--until` ajoute
les fenetres manquantes. Une relecture versionnee des deux dernieres heures
(14jours en daily) capte les corrections recentes selon la cadence configuree.
Le daily conserve son historique depuis1970.
Les trois resolutions ont des registres distincts ; les bougies non closes sont
differees, y compris les bougies horaires decalees selon les sessions.

```sh
python scripts/collect_global.py --config config/global-backfill.yaml \
  --work state/global --until 2026-09-15T16:00:00Z --execute
```

Sans `--execute`, cette commande planifie seulement. `--max-batches 1` borne un
test reseau sans retirer les autres symboles de la file. Un resultat partiel ne
prouve pas une couverture complete. Ce CLI collecte seulement ; le pipeline
ci-dessous ajoute l'export ferme et la publication du registre global.
Le profil global conserve les cotations source sans ajustement client des
dividendes. La serie daily reconstruite avant splits porte explicitement le
label `as_traded_reconstructed`, pas celui de brut fournisseur certifie.

```sh
python scripts/global_archive_pipeline.py --config config/global-backfill.yaml \
  --transport-config config/transport.yaml --work state/global \
  --run-id passage-001 --until 2026-09-15T16:00:00Z \
  --parent /absolute/path/published.json --max-batches 1 --execute --publish
```

Chaque nouveau passage a un `--run-id` distinct et prend le recu publie du
passage precedent comme parent. Conserver le meme `--work`. Pour reprendre
un passage interrompu, reutiliser exactement son ID, sa configuration et ses
arguments : l'intent, le lot ferme et la preparation deja durable sont reutilises.
Sans `--publish`, le lot reste prepare localement et doit etre publie avant de
commencer le suivant. `--bootstrap` remplace le parent uniquement pour une
premiere archive. Aucun de ces modes n'active de scheduling.

L'export global n'ajoute que les nouveaux lots observes ; les lots fermes ne
sont pas reconstruits a chaque passage. Les checkpoints v2 utilisent un petit
index et des fragments gzip content-addresses, bornes par la configuration.
Une reprise restaure seulement cet index et ses fragments, dans une transaction
locale unique ; un fragment absent ou corrompu refuse toute la restauration.
Le pin et la selection sont recontroles si la destination existe deja. Les
checkpoints v1 restent lisibles par le nouveau client ; un ancien client v1 ne
peut pas reprendre un checkpoint v2. Les anciennes releases restent intactes.
Une relecture bornee ne garantit ni toutes les revisions anciennes, ni la
completude des sessions ; ces deux qualifications restent distinctes.

Pour une fenetre explicite et un univers valide :

```sh
python scripts/incremental_collector.py --config config/manual.yaml --store state/collector \
  collect --universe /absolute/path/validated-universe.csv \
  --start 2026-09-15T12:00:00Z --end 2026-09-15T13:00:00Z --interval 1m
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
La date de generation de l'archive est distincte de l'horizon des donnees : un
rattrapage ancien ne recule pas le catalogue. Cette date est figee dans l'intent
et conservee lors des reprises, sans retelechargement du lot ferme.

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
Publication, restauration, delta et compaction ont aussi ete verifies sur GitHub.
Le test reel de compaction conserve le snapshot logique et ne transfere aucun
nouvel objet a un client deja a jour ; les metadonnees restent telechargees.
Un producteur neuf a aussi repris le checkpoint et collecte seulement l'heure
manquante ; relancer le lot publie n'effectue aucun nouvel appel fournisseur.
La selectivite reste au niveau fichier/pack : un fichier minuscule peut demander
un pack entier. Augmenter les packs reduit les objets mais amplifie ces lectures.
L'ancien script de publication non chiffree est desactive.

`backfill_sec.py` exporte des tables existantes en lecture seule vers Parquet/Zstd.
Les huit tables SEC autorisees et les budgets figurent dans
`config/backfill.example.yaml`. Les transactions/detentions d'inities sont
selectionnees par `reported_at` ; les six autres tables par `filing_date`.
Les champs originaux, y compris les dates nulles et unites13F, sont conserves.
Chaque table est lue dans sa propre transaction coherente, pas dans un snapshot
global simultane. Les lots sont bornes et ne certifient pas la couverture PIT.
`backfill_parquet.py` reutilise des exports dates via rsync et controle leurs hashes.
Ni base active, ni WAL, ni modification de permissions du serveur.

Un snapshot prouve l'identite de ses octets, pas une couverture PIT complete.
Les acquisitions courantes restent classees `provider_current_non_pit` ; les
compositions d'indices, corporate actions et revisions ont leurs propres contrats.
`pit_records.py` selectionne les revisions selon connaissance et validite, y compris
corrections et retraits. Il ne fabrique pas les dates manquantes.
`config/datasets.yaml` distingue les adaptateurs presents des collecteurs et
historiques encore requis : leur inventaire ne signifie pas leur acquisition.

`normalize_pit_inputs.py` transforme les recus de barres epingles en observations
de corporate actions et les CSV d'indices en snapshots ponctuels. Le document
source est conserve par SHA. Une date effective ne devient jamais une date
d'annonce : la connaissance reste bornee par l'observation locale. Un snapshot
mensuel ne remplit pas les jours manquants. `--bootstrap` exige un premier lot
explicite ; ensuite `--history JSONL SHA256` fournit chaque delta precedent epingle
pour poursuivre les revisions sans reemettre l'historique.

Les buckets de prix entierement nuls restent des absences, sans barre synthetique.
Les reponses partielles/incoherentes sont mises en quarantaine et conservees dans
les exports avec checkpoint. La qualification calendaires, sessions et couverture
PIT complete reste independante de la reussite du transport.
