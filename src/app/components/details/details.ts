import { Component, OnInit, computed, signal } from '@angular/core';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { DatePipe } from '@angular/common';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatSnackBar } from '@angular/material/snack-bar';
import { ToyService } from '../../../services/toy.service';
import { UserService } from '../../../services/user.service';
import { ReviewService } from '../../../services/review.service';
import { ToyModel } from '../../../models/toy.model';
import { ReviewModel } from '../../../models/review.model';
import { targetGroupLabel } from '../../../utils/target-group.util';

@Component({
  selector: 'app-details',
  imports: [RouterLink, DatePipe, MatCardModule, MatButtonModule, MatIconModule],
  templateUrl: './details.html',
  styleUrl: './details.css',
})
export class Details implements OnInit {
  readonly imageBaseUrl = 'https://toy.pequla.com';
  readonly stars = [1, 2, 3, 4, 5];
  readonly targetGroupLabel = targetGroupLabel;

  toy = signal<ToyModel | null>(null);
  loading = signal(true);
  error = signal<string | null>(null);

  reviews = signal<ReviewModel[]>([]);
  averageRating = signal<number | null>(null);

  averageRatingRounded = computed(() => {
    const avg = this.averageRating();
    return avg == null ? null : Math.round(avg * 2) / 2;
  });

  reservationMessage = signal<string | null>(null);
  reservationError = signal<string | null>(null);

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private snackBar: MatSnackBar
  ) {}

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

  averageStarIcon(star: number): string {
    const avg = this.averageRatingRounded();
    if (avg == null) {
      return 'star_border';
    }
    if (avg >= star) {
      return 'star';
    }
    if (avg >= star - 0.5) {
      return 'star_half';
    }
    return 'star_border';
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
      UserService.createReservation(toy);
      this.reservationMessage.set('Igračka je rezervisana i dodata u vašu korpu.');
      this.snackBar.open('Igračka je dodata u korpu.', 'Zatvori', { duration: 3000 });
    } catch (err) {
      this.reservationError.set(
        err instanceof Error ? err.message : 'Rezervacija nije uspela.'
      );
    }
  }
}