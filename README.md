# DistributedTaskScheduler

Projektni zadatak za kolegij Raspodijeljeni sustavi.

Ideja je napraviti sustav koji prima zadatke i raspoređuje ih na više worker čvorova koji rade paralelno. Scheduler odlučuje tko što radi, workeri samo izvršavaju i javljaju natrag rezultat. Ako jedan worker crkne, zadatak ide drugome.

## Kako to radi

Dva tipa servisa:

- **scheduler** — prima zadatke od klijenta, drži red čekanja, prati koji su workeri živi i šalje im posao
- **worker** — pokreće se kao zaseban servis (može ih biti više), pri pokretanju se registrira kod schedulera, šalje heartbeat svakih 5 sekundi i asinkrono izvršava zadatke

Scheduler sam nikad ne izvršava zadatke, samo koordinira.

## Što je napravljeno

- [x] Osnovna struktura projekta
- [x] Scheduler prima zadatke i drži evidenciju (`/submit-task`, `/tasks`)
- [x] Worker prima i asinkrono izvršava zadatke (`/execute`, `/status`)
- [x] Scheduler šalje zadatke workeru putem HTTP (`httpx`)
- [x] Registracija workera pri pokretanju + heartbeat mehanizam
- [ ] Load balancing (least-loaded)
- [ ] Fault tolerance — re-dodjela zadataka kad worker padne
- [ ] Docker

## Pokretanje

Instaliraj ovisnosti:
```bash
pip install -r requirements.txt
```

Scheduler (port 8000) — pokreni prvi:
```bash
uvicorn scheduler.main:app --host 0.0.0.0 --port 8000 --reload
```

Worker (port 8001) — pokreni nakon schedulera:
```bash
uvicorn worker.main:app --host 0.0.0.0 --port 8001 --reload
```

Za drugi worker na drugom portu:
```bash
set WORKER_PORT=8002 && uvicorn worker.main:app --host 0.0.0.0 --port 8002 --reload
```

Swagger UI:
- scheduler → http://localhost:8000/docs
- worker → http://localhost:8001/docs

## Napomene

- Scheduler treba biti pokrenut prije workera (worker se pri startu registrira)
- Ako scheduler nije dostupan, worker će pokušati 5 puta s razmakom od 2 sekunde
- Heartbeat timeout je 15 sekundi — ako worker ne javi, scheduler ga označava kao mrtvog
