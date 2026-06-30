# DistributedTaskScheduler

Aplikacija namijenjena za raspodjelu i izvršavanje zadataka u raspodijeljenom okruženju. Sustav omogućuje slanje, planiranje i obradu zadataka kroz mrežu neovisnih radnih čvorova (worker nodes), čime se postiže veća učinkovitost, skalabilnost i otpornost na kvarove.

## Cilj projekta

Ravnomjerno raspodijeliti radno opterećenje te osigurati pouzdano izvršavanje zadataka čak i u uvjetima promjenjivog opterećenja ili parcijalnih kvarova sustava.

## Arhitektura

Sustav se sastoji od:

- **Scheduler (centralni planer)** – prima zadatke, vodi evidenciju dostupnih radnih čvorova i dinamički im dodjeljuje zadatke na temelju trenutnog opterećenja i dostupnosti.
- **Worker Nodes (radni čvorovi)** – izvršavaju zadatke neovisno, prijavljuju se planeru i periodički javljaju svoje stanje (heartbeat).

Planer sam ne izvršava zadatke, već isključivo koordinira raspodjelu rada.

## Status razvoja

Projekt je u izradi. Trenutno implementirano:

- [x] Inicijalna struktura projekta

Plan daljnjeg razvoja dokumentiran je kroz commit povijest.

## Pokretanje

_Upute za pokretanje bit će dodane kako se servisi budu implementirali._
