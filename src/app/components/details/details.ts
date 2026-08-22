import { Component, OnInit, signal } from '@angular/core';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { ToyService } from '../../../services/toy.service';
import { UserService } from '../../../services/user.service';
import { ReviewService } from '../../../services/review.service';
import { ToyModel } from '../../../models/toy.model';
import { ReviewModel, ReviewAuthorType } from '../../../models/review.model';

@Component({
  selector: 'app-details',
  imports: [
    RouterLink,
    DatePipe,
    FormsModule,
    MatCardModule,
    MatButtonModule,
    MatIconModule,
    MatFormFieldModule,
    MatSelectModule,
    MatInputModule,
  ],
  templateUrl: './details.html',
  styleUrl: './details.css',
})
export class Details implements OnInit {
  readonly imageBaseUrl = 'https://toy.pequla.com';
  readonly stars = [1, 2, 3, 4, 5];

  toy = signal<ToyModel | null>(null);
  loading = signal(true);
  error = signal<string | null>(null);

  reviews = signal<ReviewModel[]>([]);
  averageRating = signal<number | null>(null);

  reservationMessage = signal<string | null>(null);
  reservationError = signal<string | null>(null);

  newReviewAuthorType = signal<ReviewAuthorType>('roditelj');
  newReviewRating = signal(0);
  newReviewComment = signal('');
  reviewError = signal<string | null>(null);
  submittingReview = signal(false);

  constructor(private route: ActivatedRoute, private router: Router) {}

  get isLoggedIn(): boolean {
    return UserService.getCurrentUser() !== null;
  }

  ngOnInit() {
    const permalink = this.route.snapshot.paramMap.get('permalink');
    if (!permalink) {
      this.error.set('Igračka nije pronađena.');
      this.loading.set(false);
      return;
    }
    this.loadToy(permalink);
  }

  async loadToy(permalink: string) {
    this.loading.set(true);
    this.error.set(null);
    try {
      const response = await ToyService.getToyByPermalink(permalink);
      this.toy.set(response.data);
      this.loadReviews();
    } catch (err) {
      this.error.set('Ne mogu da učitam igračku. Pokušajte ponovo.');
    } finally {
      this.loading.set(false);
    }
  }

  loadReviews() {
    const toy = this.toy();
    if (!toy) {
      return;
    }
    this.reviews.set(ReviewService.getReviewsForToy(toy.toyId));
    this.averageRating.set(ReviewService.getAverageRating(toy.toyId));
  }

  onReserve() {
    this.reservationError.set(null);
    this.reservationMessage.set(null);

    if (!this.isLoggedIn) {
      this.router.navigateByUrl('/login');
      return;
    }

    const toy = this.toy();
    if (!toy) {
      return;
    }

    try {
      UserService.createReservation(toy.toyId, toy.name, toy.price, toy.imageUrl);
      this.reservationMessage.set('Igračka je rezervisana i dodata u vašu korpu.');
    } catch (err) {
      this.reservationError.set(
        err instanceof Error ? err.message : 'Rezervacija nije uspela.'
      );
    }
  }

  setNewReviewRating(rating: number) {
    this.newReviewRating.set(rating);
  }

  onSubmitReview() {
    this.reviewError.set(null);
    const toy = this.toy();
    if (!toy) {
      return;
    }

    this.submittingReview.set(true);
    try {
      ReviewService.addReview(
        toy.toyId,
        this.newReviewAuthorType(),
        this.newReviewRating(),
        this.newReviewComment()
      );
      this.newReviewComment.set('');
      this.newReviewRating.set(0);
      this.loadReviews();
    } catch (err) {
      this.reviewError.set(err instanceof Error ? err.message : 'Slanje recenzije nije uspelo.');
    } finally {
      this.submittingReview.set(false);
    }
  }
}