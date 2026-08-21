import { signal } from '@angular/core';
import { UserModel } from '../models/user.model';
import { ReservationModel, ReservationStatus } from '../models/reservation.model';

const USERS_KEY = 'toybox_users';
const CURRENT_USER_KEY = 'toybox_current_user';
const RESERVATIONS_KEY = 'toybox_reservations';

async function hashPassword(password: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(password);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
}

function generateId(): string {
  return crypto.randomUUID();
}

function getUsers(): UserModel[] {
  const raw = localStorage.getItem(USERS_KEY);
  return raw ? JSON.parse(raw) : [];
}

function saveUsers(users: UserModel[]): void {
  localStorage.setItem(USERS_KEY, JSON.stringify(users));
}

function getReservationsRaw(): ReservationModel[] {
  const raw = localStorage.getItem(RESERVATIONS_KEY);
  return raw ? JSON.parse(raw) : [];
}

function saveReservations(reservations: ReservationModel[]): void {
  localStorage.setItem(RESERVATIONS_KEY, JSON.stringify(reservations));
}

export class UserService {
  static currentUser = signal<UserModel | null>(UserService.loadCurrentUserFromStorage());

  private static loadCurrentUserFromStorage(): UserModel | null {
    const raw = localStorage.getItem(CURRENT_USER_KEY);
    return raw ? JSON.parse(raw) : null;
  }

  static async signup(
    email: string,
    password: string,
    firstName: string,
    lastName: string
  ): Promise<UserModel> {
    const users = getUsers();
    const existing = users.find((u) => u.email.toLowerCase() === email.toLowerCase());
    if (existing) {
      throw new Error('Korisnik sa ovim email-om već postoji.');
    }

    const passwordHash = await hashPassword(password);
    const newUser: UserModel = {
      userId: generateId(),
      email,
      passwordHash,
      firstName,
      lastName,
    };

    users.push(newUser);
    saveUsers(users);
    UserService.setCurrentUser(newUser);
    return newUser;
  }

  static async login(email: string, password: string): Promise<UserModel> {
    const users = getUsers();
    const user = users.find((u) => u.email.toLowerCase() === email.toLowerCase());
    if (!user) {
      throw new Error('Pogrešan email ili lozinka.');
    }

    const passwordHash = await hashPassword(password);
    if (passwordHash !== user.passwordHash) {
      throw new Error('Pogrešan email ili lozinka.');
    }

    UserService.setCurrentUser(user);
    return user;
  }

  static logout(): void {
    localStorage.removeItem(CURRENT_USER_KEY);
    UserService.currentUser.set(null);
  }

  private static setCurrentUser(user: UserModel): void {
    localStorage.setItem(CURRENT_USER_KEY, JSON.stringify(user));
    UserService.currentUser.set(user);
  }

  static getCurrentUser(): UserModel | null {
    return UserService.currentUser();
  }

  static createReservation(
    toyId: number,
    toyName: string,
    toyPrice: number,
    toyImageUrl: string
  ): ReservationModel {
    const user = UserService.getCurrentUser();
    if (!user) {
      throw new Error('Morate biti prijavljeni da biste napravili rezervaciju.');
    }

    const reservations = getReservationsRaw();
    const newReservation: ReservationModel = {
      reservationId: generateId(),
      userId: user.userId,
      toyId,
      toyName,
      toyPrice,
      toyImageUrl,
      status: 'rezervisano',
      reservationDate: new Date().toISOString(),
    };

    reservations.push(newReservation);
    saveReservations(reservations);
    return newReservation;
  }

  static getReservationsForUser(userId: string): ReservationModel[] {
    return getReservationsRaw().filter((r) => r.userId === userId);
  }

  static updateReservationStatus(reservationId: string, status: ReservationStatus): void {
    const reservations = getReservationsRaw();
    const reservation = reservations.find((r) => r.reservationId === reservationId);
    if (!reservation) {
      throw new Error('Rezervacija nije pronađena.');
    }

    reservation.status = status;
    saveReservations(reservations);
  }

  static deleteReservation(reservationId: string): void {
    const reservations = getReservationsRaw().filter((r) => r.reservationId !== reservationId);
    saveReservations(reservations);
  }

  static rateReservation(reservationId: string, rating: number): void {
    const reservations = getReservationsRaw();
    const reservation = reservations.find((r) => r.reservationId === reservationId);
    if (!reservation) {
      throw new Error('Rezervacija nije pronađena.');
    }
    if (reservation.status !== 'pristiglo') {
      throw new Error('Ocenjivanje je moguće samo za rezervacije sa statusom "pristiglo".');
    }

    reservation.rating = rating;
    saveReservations(reservations);
  }
}