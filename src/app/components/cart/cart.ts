import { Component, OnInit, computed, signal } from '@angular/core';
import { DatePipe } from '@angular/common';
import { RouterLink } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatChipsModule } from '@angular/material/chips';
import { UserService } from '../../../services/user.service';
import { ReservationModel } from '../../../models/reservation.model';

@Component({
  selector: 'app-cart',
  imports: [RouterLink, DatePipe, MatCardModule, MatButtonModule, MatIconModule, MatChipsModule],
  templateUrl: './cart.html',
  styleUrl: './cart.css',
})
export class Cart implements OnInit {
  readonly imageBaseUrl = 'https://toy.pequla.com';
  readonly stars = [1, 2, 3, 4, 5];

  reservations = signal<ReservationModel[]>([]);
  errorMessage = signal<string | null>(null);

  totalPrice = computed(() =>
    this.reservations()
      .filter((r) => r.status !== 'otkazano')
      .reduce((sum, r) => sum + r.toyPrice, 0)
  );

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

  removeFromCart(reservationId: string) {
    this.errorMessage.set(null);
    try {
      UserService.deleteReservation(reservationId);
      this.loadReservations();
    } catch (err) {
      this.errorMessage.set(err instanceof Error ? err.message : 'Brisanje nije uspelo.');
    }
  }

  rate(reservationId: string, rating: number) {
    this.errorMessage.set(null);
    try {
      UserService.rateReservation(reservationId, rating);
      this.loadReservations();
    } catch (err) {
      this.errorMessage.set(err instanceof Error ? err.message : 'Ocenjivanje nije uspelo.');
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