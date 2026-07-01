# DistributedTaskScheduler

Projektni zadatak za kolegij Raspodijeljeni sustavi.

Ideja je napraviti sustav koji prima zadatke i raspoređuje ih na više worker čvorova koji rade paralelno. Scheduler odlučuje tko što radi, workeri samo izvršavaju i javljaju natrag rezultat. Ako jedan worker crkne, zadatak ide drugome.

## Kako to radi

Dva tipa servisa:

- **scheduler** — prima zadatke od klijenta, drži red čekanja, bira workera i šalje mu posao
- **worker** — pokreće se kao zaseban servis (može ih biti više), prima zadatak, izvršava ga asinkrono i javlja status

Scheduler sam nikad ne izvršava zadatke, samo koordinira.

## Što je trenutno napravljeno

- [x] Osnovna struktura projekta
- [x] Scheduler prima zadatke i drži evidenciju (`/submit-task`, `/tasks`)
- [x] Worker prima i asinkrono izvršava zadatke (`/execute`, `/status`)
- [ ] Scheduler šalje zadatke workeru (HTTP)
- [ ] Registracija workera i heartbeat
- [ ] Load balancing (least-loaded)
- [ ] Fault tolerance — re-dodjela zadataka kad worker padne
- [ ] Docker

## Pokretanje

```bash
pip install -r requirements.txt
```

U jednom terminalu:
```bash
uvicorn scheduler.main:app --host 0.0.0.0 --port 8000 --reload
```

U drugom:
```bash
uvicorn worker.main:app --host 0.0.0.0 --port 8001 --reload
```

Swagger UI:
- scheduler → http://localhost:8000/docs
- worker → http://localhost:8001/docs
