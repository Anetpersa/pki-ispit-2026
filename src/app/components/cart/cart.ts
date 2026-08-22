import { Component, OnInit, computed, signal } from '@angular/core';
import { DatePipe } from '@angular/common';
import { RouterLink } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatChipsModule } from '@angular/material/chips';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { FormsModule } from '@angular/forms';
import { UserService } from '../../../services/user.service';
import { ReviewService } from '../../../services/review.service';
import { ReservationModel } from '../../../models/reservation.model';
import { ReviewAuthorType } from '../../../models/review.model';
import { targetGroupLabel } from '../../../utils/target-group.util';

@Component({
  selector: 'app-cart',
  imports: [
    RouterLink,
    DatePipe,
    MatCardModule,
    MatButtonModule,
    MatIconModule,
    MatChipsModule,
    MatFormFieldModule,
    MatSelectModule,
    MatInputModule,
    FormsModule,
  ],
  templateUrl: './cart.html',
  styleUrl: './cart.css',
})
export class Cart implements OnInit {
  readonly stars = [1, 2, 3, 4, 5];
  readonly targetGroupLabel = targetGroupLabel;
  readonly imageBaseUrl = 'https://toy.pequla.com';

  reservations = signal<ReservationModel[]>([]);
  errorMessage = signal<string | null>(null);

  totalPrice = computed(() =>
    this.reservations()
      .filter((r) => r.status !== 'otkazano')
      .reduce((sum, r) => sum + r.toyPrice, 0)
  );

  ratingForReservationId = signal<string | null>(null);
  newRating = signal(0);
  newReviewAuthorType = signal<ReviewAuthorType>('roditelj');
  newReviewComment = signal('');
  ratingError = signal<string | null>(null);
  submittingRating = signal(false);

  private userId = '';

  ngOnInit() {
    const user = UserService.getCurrentUser();
    if (!user) {
      return;
    }
    this.userId = user.userId;
    this.loadReservations();
  }

  loadReservations() {
    const list = UserService.getReservationsForUser(this.userId);
    list.sort((a, b) => new Date(b.reservationDate).getTime() - new Date(a.reservationDate).getTime());
    this.reservations.set(list);
  }

  cancelReservation(reservationId: string) {
    this.errorMessage.set(null);
    try {
      UserService.updateReservationStatus(reservationId, 'otkazano');
      this.loadReservations();
    } catch (err) {
      this.errorMessage.set(err instanceof Error ? err.message : 'Otkazivanje nije uspelo.');
    }
  }

  markAsArrived(reservationId: string) {
    this.errorMessage.set(null);
    try {
      UserService.markReservationArrived(reservationId);
      this.loadReservations();
    } catch (err) {
      this.errorMessage.set(
        err instanceof Error ? err.message : 'Označavanje kao pristiglo nije uspelo.'
      );
    }
  }

  removeFromCart(reservationId: string) {
    this.errorMessage.set(null);
    try {
      UserService.deleteReservation(reservationId);
      this.loadReservations();
    } catch (err) {
      this.errorMessage.set(err instanceof Error ? err.message : 'Brisanje nije uspelo.');
    }
  }

  openRatingForm(reservationId: string) {
    this.ratingError.set(null);
    this.ratingForReservationId.set(reservationId);
    this.newRating.set(0);
    this.newReviewAuthorType.set('roditelj');
    this.newReviewComment.set('');
  }

  closeRatingForm() {
    this.ratingForReservationId.set(null);
  }

  setNewRating(rating: number) {
    this.newRating.set(rating);
  }

  submitRating(reservation: ReservationModel) {
    this.ratingError.set(null);

    if (this.newRating() < 1) {
      this.ratingError.set('Izaberite ocenu od 1 do 5 zvezdica.');
      return;
    }
    if (!this.newReviewComment().trim()) {
      this.ratingError.set('Unesite kratak komentar.');
      return;
    }

    this.submittingRating.set(true);
    try {
      UserService.rateReservation(reservation.reservationId, this.newRating());
      ReviewService.addReview(
        reservation.toyId,
        this.newReviewAuthorType(),
        this.newRating(),
        this.newReviewComment()
      );
      this.closeRatingForm();
      this.loadReservations();
    } catch (err) {
      this.ratingError.set(err instanceof Error ? err.message : 'Ocenjivanje nije uspelo.');
    } finally {
      this.submittingRating.set(false);
    }
  }

  statusLabel(status: ReservationModel['status']): string {
    switch (status) {
      case 'rezervisano':
        return 'Rezervisano';
      case 'pristiglo':
        return 'Pristiglo';
      case 'otkazano':
        return 'Otkazano';
    }
  }
}