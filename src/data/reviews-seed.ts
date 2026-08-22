import { ReviewModel } from '../models/review.model';

/**
 * Statički (seed) skup recenzija za predefinisani skup igračaka.
 * Ovo simulira postojeće recenzije korisnika (dece i roditelja) jer
 * toy.pequla.com API ne vraća recenzije - one su isključivo lokalna simulacija.
 */
export const REVIEWS_SEED: ReviewModel[] = [
  {
    "reviewId": "seed-1",
    "toyId": 1,
    "authorName": "Mila",
    "authorType": "dete",
    "rating": 4,
    "comment": "Igram se ovim svaki dan, super je!",
    "date": "2024-01-01"
  },
  {
    "reviewId": "seed-2",
    "toyId": 1,
    "authorName": "Nemanja R.",
    "authorType": "roditelj",
    "rating": 4,
    "comment": "Materijal je bezbedan, brzo stiže i dobro je upakovano.",
    "date": "2024-02-06"
  },
  {
    "reviewId": "seed-3",
    "toyId": 1,
    "authorName": "Ana",
    "authorType": "dete",
    "rating": 4,
    "comment": "Malo je teško za sastavljanje ali je zabavno.",
    "date": "2024-03-11"
  },
  {
    "reviewId": "seed-4",
    "toyId": 2,
    "authorName": "Jelena S.",
    "authorType": "roditelj",
    "rating": 4,
    "comment": "Materijal je bezbedan, brzo stiže i dobro je upakovano.",
    "date": "2024-02-04"
  },
  {
    "reviewId": "seed-5",
    "toyId": 2,
    "authorName": "Teodora",
    "authorType": "dete",
    "rating": 4,
    "comment": "Malo je teško za sastavljanje ali je zabavno.",
    "date": "2024-03-09"
  },
  {
    "reviewId": "seed-6",
    "toyId": 3,
    "authorName": "Sofija",
    "authorType": "dete",
    "rating": 4,
    "comment": "Malo je teško za sastavljanje ali je zabavno.",
    "date": "2024-03-07"
  },
  {
    "reviewId": "seed-7",
    "toyId": 3,
    "authorName": "Bojan Đ.",
    "authorType": "roditelj",
    "rating": 3,
    "comment": "Malo je skuplje nego što sam očekivao/la, ali vredi.",
    "date": "2024-04-12"
  },
  {
    "reviewId": "seed-8",
    "toyId": 4,
    "authorName": "Ivana K.",
    "authorType": "roditelj",
    "rating": 3,
    "comment": "Malo je skuplje nego što sam očekivao/la, ali vredi.",
    "date": "2024-04-10"
  },
  {
    "reviewId": "seed-9",
    "toyId": 4,
    "authorName": "Iva",
    "authorType": "dete",
    "rating": 4,
    "comment": "Volim da se igram sa bratom ovim.",
    "date": "2024-05-15"
  },
  {
    "reviewId": "seed-10",
    "toyId": 4,
    "authorName": "Tijana M.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Odlična igračka, kupujem i za rođendan drugarici.",
    "date": "2024-06-20"
  },
  {
    "reviewId": "seed-11",
    "toyId": 5,
    "authorName": "Mia",
    "authorType": "dete",
    "rating": 4,
    "comment": "Volim da se igram sa bratom ovim.",
    "date": "2024-05-13"
  },
  {
    "reviewId": "seed-12",
    "toyId": 5,
    "authorName": "Aleksandar N.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Odlična igračka, kupujem i za rođendan drugarici.",
    "date": "2024-06-18"
  },
  {
    "reviewId": "seed-13",
    "toyId": 6,
    "authorName": "Vesna J.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Odlična igračka, kupujem i za rođendan drugarici.",
    "date": "2024-06-16"
  },
  {
    "reviewId": "seed-14",
    "toyId": 6,
    "authorName": "Sara",
    "authorType": "dete",
    "rating": 4,
    "comment": "Mogu satima da se igram ovim.",
    "date": "2024-07-21"
  },
  {
    "reviewId": "seed-15",
    "toyId": 7,
    "authorName": "Ema",
    "authorType": "dete",
    "rating": 4,
    "comment": "Mogu satima da se igram ovim.",
    "date": "2024-07-19"
  },
  {
    "reviewId": "seed-16",
    "toyId": 7,
    "authorName": "Igor S.",
    "authorType": "roditelj",
    "rating": 4,
    "comment": "Dete je oduševljeno, kvalitet je odličan za tu cenu.",
    "date": "2024-08-24"
  },
  {
    "reviewId": "seed-17",
    "toyId": 7,
    "authorName": "Mila",
    "authorType": "dete",
    "rating": 4,
    "comment": "Baš mi se dopada, bojе su prelepe.",
    "date": "2024-09-02"
  },
  {
    "reviewId": "seed-18",
    "toyId": 8,
    "authorName": "Snežana P.",
    "authorType": "roditelj",
    "rating": 4,
    "comment": "Dete je oduševljeno, kvalitet je odličan za tu cenu.",
    "date": "2024-08-22"
  },
  {
    "reviewId": "seed-19",
    "toyId": 8,
    "authorName": "Ana",
    "authorType": "dete",
    "rating": 4,
    "comment": "Baš mi se dopada, bojе su prelepe.",
    "date": "2024-09-27"
  },
  {
    "reviewId": "seed-20",
    "toyId": 9,
    "authorName": "Teodora",
    "authorType": "dete",
    "rating": 4,
    "comment": "Baš mi se dopada, bojе su prelepe.",
    "date": "2024-09-25"
  },
  {
    "reviewId": "seed-21",
    "toyId": 9,
    "authorName": "Predrag K.",
    "authorType": "roditelj",
    "rating": 3,
    "comment": "Preporučujem, razvija maštu i motoriku kod deteta.",
    "date": "2024-10-03"
  },
  {
    "reviewId": "seed-22",
    "toyId": 10,
    "authorName": "Milena R.",
    "authorType": "roditelj",
    "rating": 3,
    "comment": "Preporučujem, razvija maštu i motoriku kod deteta.",
    "date": "2024-10-01"
  },
  {
    "reviewId": "seed-23",
    "toyId": 10,
    "authorName": "Jovana",
    "authorType": "dete",
    "rating": 4,
    "comment": "Ovo mi je najdraža igračka do sada!",
    "date": "2024-11-06"
  },
  {
    "reviewId": "seed-24",
    "toyId": 10,
    "authorName": "Marija P.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Dete se brzo zasitilo, očekivali smo više.",
    "date": "2024-12-11"
  },
  {
    "reviewId": "seed-25",
    "toyId": 11,
    "authorName": "Iva",
    "authorType": "dete",
    "rating": 4,
    "comment": "Ovo mi je najdraža igračka do sada!",
    "date": "2024-11-04"
  },
  {
    "reviewId": "seed-26",
    "toyId": 11,
    "authorName": "Nemanja R.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Dete se brzo zasitilo, očekivali smo više.",
    "date": "2024-12-09"
  },
  {
    "reviewId": "seed-27",
    "toyId": 12,
    "authorName": "Jelena S.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Dete se brzo zasitilo, očekivali smo više.",
    "date": "2024-12-07"
  },
  {
    "reviewId": "seed-28",
    "toyId": 12,
    "authorName": "Lena",
    "authorType": "dete",
    "rating": 4,
    "comment": "Super je za nošenje na spavanje.",
    "date": "2024-01-12"
  },
  {
    "reviewId": "seed-29",
    "toyId": 13,
    "authorName": "Sara",
    "authorType": "dete",
    "rating": 4,
    "comment": "Super je za nošenje na spavanje.",
    "date": "2024-01-10"
  },
  {
    "reviewId": "seed-30",
    "toyId": 13,
    "authorName": "Bojan Đ.",
    "authorType": "roditelj",
    "rating": 4,
    "comment": "Traje dugo i lako se održava čistim.",
    "date": "2024-02-15"
  },
  {
    "reviewId": "seed-31",
    "toyId": 13,
    "authorName": "Ema",
    "authorType": "dete",
    "rating": 4,
    "comment": "Igram se ovim svaki dan, super je!",
    "date": "2024-03-20"
  },
  {
    "reviewId": "seed-32",
    "toyId": 14,
    "authorName": "Ivana K.",
    "authorType": "roditelj",
    "rating": 4,
    "comment": "Traje dugo i lako se održava čistim.",
    "date": "2024-02-13"
  },
  {
    "reviewId": "seed-33",
    "toyId": 14,
    "authorName": "Mila",
    "authorType": "dete",
    "rating": 4,
    "comment": "Igram se ovim svaki dan, super je!",
    "date": "2024-03-18"
  },
  {
    "reviewId": "seed-34",
    "toyId": 15,
    "authorName": "Ana",
    "authorType": "dete",
    "rating": 4,
    "comment": "Igram se ovim svaki dan, super je!",
    "date": "2024-03-16"
  },
  {
    "reviewId": "seed-35",
    "toyId": 15,
    "authorName": "Aleksandar N.",
    "authorType": "roditelj",
    "rating": 3,
    "comment": "Materijal je bezbedan, brzo stiže i dobro je upakovano.",
    "date": "2024-04-21"
  },
  {
    "reviewId": "seed-36",
    "toyId": 16,
    "authorName": "Vesna J.",
    "authorType": "roditelj",
    "rating": 3,
    "comment": "Materijal je bezbedan, brzo stiže i dobro je upakovano.",
    "date": "2024-04-19"
  },
  {
    "reviewId": "seed-37",
    "toyId": 16,
    "authorName": "Sofija",
    "authorType": "dete",
    "rating": 4,
    "comment": "Malo je teško za sastavljanje ali je zabavno.",
    "date": "2024-05-24"
  },
  {
    "reviewId": "seed-38",
    "toyId": 16,
    "authorName": "Katarina Đ.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Malo je skuplje nego što sam očekivao/la, ali vredi.",
    "date": "2024-06-02"
  },
  {
    "reviewId": "seed-39",
    "toyId": 17,
    "authorName": "Jovana",
    "authorType": "dete",
    "rating": 4,
    "comment": "Malo je teško za sastavljanje ali je zabavno.",
    "date": "2024-05-22"
  },
  {
    "reviewId": "seed-40",
    "toyId": 17,
    "authorName": "Igor S.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Malo je skuplje nego što sam očekivao/la, ali vredi.",
    "date": "2024-06-27"
  },
  {
    "reviewId": "seed-41",
    "toyId": 18,
    "authorName": "Snežana P.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Malo je skuplje nego što sam očekivao/la, ali vredi.",
    "date": "2024-06-25"
  },
  {
    "reviewId": "seed-42",
    "toyId": 18,
    "authorName": "Mia",
    "authorType": "dete",
    "rating": 4,
    "comment": "Volim da se igram sa bratom ovim.",
    "date": "2024-07-03"
  },
  {
    "reviewId": "seed-43",
    "toyId": 19,
    "authorName": "Lena",
    "authorType": "dete",
    "rating": 4,
    "comment": "Volim da se igram sa bratom ovim.",
    "date": "2024-07-01"
  },
  {
    "reviewId": "seed-44",
    "toyId": 19,
    "authorName": "Predrag K.",
    "authorType": "roditelj",
    "rating": 4,
    "comment": "Odlična igračka, kupujem i za rođendan drugarici.",
    "date": "2024-08-06"
  },
  {
    "reviewId": "seed-45",
    "toyId": 19,
    "authorName": "Sara",
    "authorType": "dete",
    "rating": 4,
    "comment": "Mogu satima da se igram ovim.",
    "date": "2024-09-11"
  },
  {
    "reviewId": "seed-46",
    "toyId": 20,
    "authorName": "Milena R.",
    "authorType": "roditelj",
    "rating": 4,
    "comment": "Odlična igračka, kupujem i za rođendan drugarici.",
    "date": "2024-08-04"
  },
  {
    "reviewId": "seed-47",
    "toyId": 20,
    "authorName": "Ema",
    "authorType": "dete",
    "rating": 4,
    "comment": "Mogu satima da se igram ovim.",
    "date": "2024-09-09"
  },
  {
    "reviewId": "seed-48",
    "toyId": 21,
    "authorName": "Mila",
    "authorType": "dete",
    "rating": 4,
    "comment": "Mogu satima da se igram ovim.",
    "date": "2024-09-07"
  },
  {
    "reviewId": "seed-49",
    "toyId": 21,
    "authorName": "Nemanja R.",
    "authorType": "roditelj",
    "rating": 3,
    "comment": "Dete je oduševljeno, kvalitet je odličan za tu cenu.",
    "date": "2024-10-12"
  },
  {
    "reviewId": "seed-50",
    "toyId": 22,
    "authorName": "Jelena S.",
    "authorType": "roditelj",
    "rating": 3,
    "comment": "Dete je oduševljeno, kvalitet je odličan za tu cenu.",
    "date": "2024-10-10"
  },
  {
    "reviewId": "seed-51",
    "toyId": 22,
    "authorName": "Teodora",
    "authorType": "dete",
    "rating": 4,
    "comment": "Baš mi se dopada, bojе su prelepe.",
    "date": "2024-11-15"
  },
  {
    "reviewId": "seed-52",
    "toyId": 22,
    "authorName": "Milica V.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Preporučujem, razvija maštu i motoriku kod deteta.",
    "date": "2024-12-20"
  },
  {
    "reviewId": "seed-53",
    "toyId": 23,
    "authorName": "Sofija",
    "authorType": "dete",
    "rating": 4,
    "comment": "Baš mi se dopada, bojе su prelepe.",
    "date": "2024-11-13"
  },
  {
    "reviewId": "seed-54",
    "toyId": 23,
    "authorName": "Bojan Đ.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Preporučujem, razvija maštu i motoriku kod deteta.",
    "date": "2024-12-18"
  },
  {
    "reviewId": "seed-55",
    "toyId": 24,
    "authorName": "Ivana K.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Preporučujem, razvija maštu i motoriku kod deteta.",
    "date": "2024-12-16"
  },
  {
    "reviewId": "seed-56",
    "toyId": 24,
    "authorName": "Iva",
    "authorType": "dete",
    "rating": 4,
    "comment": "Ovo mi je najdraža igračka do sada!",
    "date": "2024-01-21"
  },
  {
    "reviewId": "seed-57",
    "toyId": 25,
    "authorName": "Mia",
    "authorType": "dete",
    "rating": 4,
    "comment": "Ovo mi je najdraža igračka do sada!",
    "date": "2024-01-19"
  },
  {
    "reviewId": "seed-58",
    "toyId": 25,
    "authorName": "Aleksandar N.",
    "authorType": "roditelj",
    "rating": 4,
    "comment": "Dete se brzo zasitilo, očekivali smo više.",
    "date": "2024-02-24"
  },
  {
    "reviewId": "seed-59",
    "toyId": 25,
    "authorName": "Lena",
    "authorType": "dete",
    "rating": 4,
    "comment": "Super je za nošenje na spavanje.",
    "date": "2024-03-02"
  },
  {
    "reviewId": "seed-60",
    "toyId": 26,
    "authorName": "Vesna J.",
    "authorType": "roditelj",
    "rating": 4,
    "comment": "Dete se brzo zasitilo, očekivali smo više.",
    "date": "2024-02-22"
  },
  {
    "reviewId": "seed-61",
    "toyId": 26,
    "authorName": "Sara",
    "authorType": "dete",
    "rating": 4,
    "comment": "Super je za nošenje na spavanje.",
    "date": "2024-03-27"
  },
  {
    "reviewId": "seed-62",
    "toyId": 27,
    "authorName": "Ema",
    "authorType": "dete",
    "rating": 4,
    "comment": "Super je za nošenje na spavanje.",
    "date": "2024-03-25"
  },
  {
    "reviewId": "seed-63",
    "toyId": 27,
    "authorName": "Igor S.",
    "authorType": "roditelj",
    "rating": 3,
    "comment": "Traje dugo i lako se održava čistim.",
    "date": "2024-04-03"
  },
  {
    "reviewId": "seed-64",
    "toyId": 28,
    "authorName": "Snežana P.",
    "authorType": "roditelj",
    "rating": 3,
    "comment": "Traje dugo i lako se održava čistim.",
    "date": "2024-04-01"
  },
  {
    "reviewId": "seed-65",
    "toyId": 28,
    "authorName": "Ana",
    "authorType": "dete",
    "rating": 4,
    "comment": "Igram se ovim svaki dan, super je!",
    "date": "2024-05-06"
  },
  {
    "reviewId": "seed-66",
    "toyId": 28,
    "authorName": "Ana M.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Materijal je bezbedan, brzo stiže i dobro je upakovano.",
    "date": "2024-06-11"
  },
  {
    "reviewId": "seed-67",
    "toyId": 29,
    "authorName": "Teodora",
    "authorType": "dete",
    "rating": 4,
    "comment": "Igram se ovim svaki dan, super je!",
    "date": "2024-05-04"
  },
  {
    "reviewId": "seed-68",
    "toyId": 29,
    "authorName": "Predrag K.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Materijal je bezbedan, brzo stiže i dobro je upakovano.",
    "date": "2024-06-09"
  },
  {
    "reviewId": "seed-69",
    "toyId": 30,
    "authorName": "Milena R.",
    "authorType": "roditelj",
    "rating": 5,
    "comment": "Materijal je bezbedan, brzo stiže i dobro je upakovano.",
    "date": "2024-06-07"
  },
  {
    "reviewId": "seed-70",
    "toyId": 30,
    "authorName": "Jovana",
    "authorType": "dete",
    "rating": 4,
    "comment": "Malo je teško za sastavljanje ali je zabavno.",
    "date": "2024-07-12"
  }
];