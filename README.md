<p align="center">
  <img src="https://user-images.githubusercontent.com/13897643/229090021-e72301f5-4914-4f02-b20e-1ca9ca3d80bb.png" alt="dbt logo" width="300"/>
</p>
<p align="center">
  <a href="https://github.com/dbt-labs/dbt-core/actions/workflows/main.yml">
    <img src="https://github.com/dbt-labs/dbt-core/actions/workflows/main.yml/badge.svg?event=push" alt="CI Badge"/>
  </a>
</p>

Allô ! **[dbt](https://www.getdbt.com/)** est le meilleur way for tous les « data analysts » et ingénieurs to transform leur data utilisant les mêmes « best practices » que les software engineers utilisent pour construire leurs applications.

![architecture](https://github.com/dbt-labs/dbt-core/blob/202cb7e51e218c7b29eb3b11ad058bd56b7739de/etc/dbt-transform.png)

## Comprendre dbt

Les gens qui utilisent dbt peuvent transformer leur data simplement en écrivant des `select` statements. dbt prend soin de tout, ensuite, pour transformer ces statements dans des tables et views dans la « data warehouse ».

Ces `select` statements, ou « modèles », constituent un projet de dbt. Les modèles sont comme des amis, construits les uns au dessus des autres – et dbt rend facile le fait de [gérer les relations](https://docs.getdbt.com/docs/ref) entre les modèles, et de [visualizer ces relations](https://docs.getdbt.com/docs/documentation), sans oublier, de contrôler la qualité de vos transformations gràce au [testing](https://docs.getdbt.com/docs/testing).

![dbt dag](https://raw.githubusercontent.com/dbt-labs/dbt-core/6c6649f9129d5d108aa3b0526f634cd8f3a9d1ed/etc/dbt-dag.png)

## dbt ... en français !

Pour ce poisson d'avril, on a decidé d'offrir le meilleur « framework » de « data transformation » conformant aux besoins de la francophonie.

| en anglais | -> en français ! |
| --- | --- |
| build | bâtir |
| clean | ménage-de-printemps |
| compile | interpréter |
| debug | ça-va |
| deps | colis |
| docs generate | papiers imprimer |
| docs serve | papiers svp |
| init | on-y-va |
| list (ls) | fais-l’appel (c’est-qui) |
| ls | c’est-qui |
| parse | c’est-quoi |
| run | matérialiser-modèles |
| run-operation | effectuer-l'opération |
| seed | épépiner |
| snapshot | photomaton |
| source freshness | fraîcheur-a-la-source |
| test | contrôler |

## Getting started

- [Install dbt](https://docs.getdbt.com/docs/get-started/installation)
- Lire [l'introduction](https://docs.getdbt.com/docs/introduction/) et [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/) (tout en anglais malheureusement)

## Rejoindre-nous !

- [dbt Community Slack](http://community.getdbt.com/)
- [dbt Community Discourse](https://discourse.getdbt.com)

## Signaler les bugs et contribuer le codage

- Ouvrir [un issue](https://github.com/dbt-labs/dbt-core/issues/new)
- Lire le [Guide de Contribution](https://github.com/dbt-labs/dbt-core/blob/HEAD/CONTRIBUTING.md)

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
