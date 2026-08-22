import { ReviewModel, ReviewAuthorType } from '../models/review.model';
import { REVIEWS_SEED } from '../data/reviews-seed';
import { UserService } from './user.service';

const USER_REVIEWS_KEY = 'toybox_user_reviews';

function generateId(): string {
  return crypto.randomUUID();
}

function getUserReviews(): ReviewModel[] {
  const raw = localStorage.getItem(USER_REVIEWS_KEY);
  return raw ? JSON.parse(raw) : [];
}

function saveUserReviews(reviews: ReviewModel[]): void {
  localStorage.setItem(USER_REVIEWS_KEY, JSON.stringify(reviews));
}

export class ReviewService {
  /**
   * Vraća sve recenzije za igračku (seed + korisnički dodate), sortirane od najnovije.
   */
  static getReviewsForToy(toyId: number): ReviewModel[] {
    const all = [...REVIEWS_SEED, ...getUserReviews()];
    return all
      .filter((r) => r.toyId === toyId)
      .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
  }

  /**
   * Dodaje novu recenziju za igračku. Zahteva prijavljenog korisnika -
   * autor recenzije je automatski ime i prezime trenutnog korisnika.
   */
  static addReview(
    toyId: number,
    authorType: ReviewAuthorType,
    rating: number,
    comment: string
  ): ReviewModel {
    const user = UserService.getCurrentUser();
    if (!user) {
      throw new Error('Morate biti prijavljeni da biste dodali recenziju.');
    }
    if (rating < 1 || rating > 5) {
      throw new Error('Ocena mora biti između 1 i 5.');
    }
    if (!comment.trim()) {
      throw new Error('Komentar ne sme biti prazan.');
    }

    const newReview: ReviewModel = {
      reviewId: generateId(),
      toyId,
      authorName: `${user.firstName} ${user.lastName}`,
      authorType,
      rating,
      comment: comment.trim(),
      date: new Date().toISOString(),
    };

    const userReviews = getUserReviews();
    userReviews.push(newReview);
    saveUserReviews(userReviews);

    return newReview;
  }

  /**
   * Vraća skup ID-jeva igračaka čije recenzije (komentar) sadrže dati tekst.
   * Koristi se za kriterijum pretrage "po recenzijama" na katalog stranici.
   */
  static findToyIdsByReviewText(term: string): Set<number> {
    const search = term.trim().toLowerCase();
    const matches = new Set<number>();
    if (!search) {
      return matches;
    }

    const all = [...REVIEWS_SEED, ...getUserReviews()];
    for (const review of all) {
      if (review.comment.toLowerCase().includes(search)) {
        matches.add(review.toyId);
      }
    }
    return matches;
  }

  static getAverageRating(toyId: number): number | null {
    const reviews = ReviewService.getReviewsForToy(toyId);
    if (reviews.length === 0) {
      return null;
    }
    const sum = reviews.reduce((acc, r) => acc + r.rating, 0);
    return Math.round((sum / reviews.length) * 10) / 10;
  }
}