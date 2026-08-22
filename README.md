# ToyBox — Digitalna prodavnica igračaka za decu

Prototip korisničkog interfejsa veb-aplikacije za pretragu i rezervaciju igračaka, rađen u okviru
predmeta PKI (semestralni/ispitni projekat 2026). Aplikacija je urađena u Angular-u, sa simulacijom
pozadinske logike preko TypeScript interfejsa i Angular servisa (bez pravog backend servera za
korisnike, rezervacije, profil i recenzije).

## Sadržaj

- [Opis funkcionalnosti](#opis-funkcionalnosti)
- [Tehnologije](#tehnologije)
- [Struktura projekta](#struktura-projekta)
- [Pokretanje projekta](#pokretanje-projekta)
- [Simulacija podataka](#simulacija-podataka)
- [Poznata ograničenja](#poznata-ograničenja)

## Opis funkcionalnosti

**Katalog igračaka** (`/igracke`)
- Pregled predefinisanog skupa igračaka (naziv, opis, tip, uzrast, ciljna grupa, datum
  proizvodnje, cena).
- Pretraga po više kriterijuma odvojeno: naziv/opis, tip, uzrasna grupa, ciljna grupa, opseg cene,
  opseg datuma proizvodnje i tekst iz recenzija.

**Detalji igračke** (`/igracke/:permalink`)
- Prikaz svih podataka o igrački, recenzija korisnika (dete/roditelj) i prosečne ocene.
- Rezervacija igračke (zahteva prijavu; ako korisnik nije prijavljen, biva preusmeren na login sa
  povratkom na stranicu igračke nakon prijave).

**Korpa** (`/cart`, zahteva prijavu)
- Prikaz svih rezervacija korisnika sa statusom (`rezervisano`, `pristiglo`, `otkazano`) i
  automatskim obračunom cene po stavci (cena × količina) i ukupne cene korpe.
- Izmena količine dok je rezervacija u statusu `rezervisano`.
- Otkazivanje rezervacije (`rezervisano` → `otkazano`) i označavanje kao pristiglo
  (`rezervisano` → `pristiglo`).
- Brisanje stavki iz korpe.
- Ocenjivanje igračke (1–5 zvezdica) i ostavljanje recenzije, isključivo za rezervacije u statusu
  `pristiglo`.

**Profil** (`/profil`, zahteva prijavu)
- Izmena ličnih podataka (ime, prezime, email, telefon, adresa).
- Izmena omiljenih tipova igračaka.
- Promena lozinke.

**Prijava i registracija** (`/login`, `/signup`)
- Registracija sa unosom svih podataka profila (ime, prezime, kontakt podaci, lozinka i omiljeni
  tipovi igračaka).
- Prijava postojećeg korisnika.
- Zaštita ruta `/profil` i `/cart` pomoću `authGuard` — nedostupne su neprijavljenom korisniku.

**Navigacija**
- Pretraga iz navbar-a vodi na katalog i automatski primenjuje uneti termin pretrage.
- Bedž sa brojem aktivnih (nedovršenih) rezervacija u korpi.

## Tehnologije

- Angular 22 (standalone komponente, `signal`/`computed` za reaktivno stanje)
- Angular Material (kartice, forme, chip-ovi, meniji, snackbar...)
- Axios (dohvatanje kataloga igračaka, tipova i uzrasnih grupa)
- TypeScript interfejsi za modele podataka (`ToyModel`, `UserModel`, `ReservationModel`,
  `ReviewModel`...)

## Struktura projekta