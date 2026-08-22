import { computed, signal } from '@angular/core';
import { UserModel } from '../models/user.model';
import { ReservationModel, ReservationStatus } from '../models/reservation.model';
import { ToyModel } from '../models/toy.model';

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

function loadReservationsFromStorage(): ReservationModel[] {
    const raw = localStorage.getItem(RESERVATIONS_KEY);
    return raw ? JSON.parse(raw) : [];
}

function saveReservations(reservations: ReservationModel[]): void {
    localStorage.setItem(RESERVATIONS_KEY, JSON.stringify(reservations));
}

export class UserService {
    static currentUser = signal<UserModel | null>(UserService.loadCurrentUserFromStorage());

    static reservations = signal<ReservationModel[]>(loadReservationsFromStorage());

    static activeCartCount = computed(() => {
        const user = UserService.currentUser();
        if (!user) {
            return 0;
        }
        return UserService.reservations().filter(
            (r) => r.userId === user.userId && r.status !== 'otkazano'
        ).length;
    });

    private static loadCurrentUserFromStorage(): UserModel | null {
        const raw = localStorage.getItem(CURRENT_USER_KEY);
        return raw ? JSON.parse(raw) : null;
    }

    static async signup(
        email: string,
        password: string,
        firstName: string,
        lastName: string,
        phone: string,
        address: string,
        favoriteToyTypes: number[] = []
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
            phone,
            address,
            favoriteToyTypes,
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

    static updateProfile(
        userId: string,
        updates: Partial<
            Pick<UserModel, 'firstName' | 'lastName' | 'email' | 'phone' | 'address' | 'favoriteToyTypes'>
        >
    ): UserModel {
        const users = getUsers();
        const user = users.find((u) => u.userId === userId);
        if (!user) {
            throw new Error('Korisnik nije pronađen.');
        }

        if (updates.email && updates.email.toLowerCase() !== user.email.toLowerCase()) {
            const emailTaken = users.some(
                (u) => u.userId !== userId && u.email.toLowerCase() === updates.email!.toLowerCase()
            );
            if (emailTaken) {
                throw new Error('Korisnik sa ovim email-om već postoji.');
            }
        }

        Object.assign(user, updates);
        saveUsers(users);

        if (UserService.getCurrentUser()?.userId === userId) {
            UserService.setCurrentUser(user);
        }

        return user;
    }

    static async changePassword(userId: string, newPassword: string): Promise<void> {
        const users = getUsers();
        const user = users.find((u) => u.userId === userId);
        if (!user) {
            throw new Error('Korisnik nije pronađen.');
        }

        user.passwordHash = await hashPassword(newPassword);
        saveUsers(users);

        if (UserService.getCurrentUser()?.userId === userId) {
            UserService.setCurrentUser(user);
        }
    }

    static createReservation(toy: ToyModel): ReservationModel {
        const user = UserService.getCurrentUser();
        if (!user) {
            throw new Error('Morate biti prijavljeni da biste napravili rezervaciju.');
        }

        const newReservation: ReservationModel = {
            reservationId: generateId(),
            userId: user.userId,
            toyId: toy.toyId,
            toyName: toy.name,
            toyDescription: toy.description,
            toyTypeName: toy.type.name,
            toyAgeGroupName: toy.ageGroup.name,
            toyTargetGroup: toy.targetGroup,
            toyProductionDate: toy.productionDate,
            toyPrice: toy.price,
            toyImageUrl: toy.imageUrl,
            status: 'rezervisano',
            reservationDate: new Date().toISOString(),
        };

        const reservations = [...UserService.reservations(), newReservation];
        saveReservations(reservations);
        UserService.reservations.set(reservations);
        return newReservation;
    }

    static getReservationsForUser(userId: string): ReservationModel[] {
        return UserService.reservations().filter((r) => r.userId === userId);
    }

    private static updateReservation(
        reservationId: string,
        updater: (reservation: ReservationModel) => void
    ): void {
        const reservations = UserService.reservations();
        const reservation = reservations.find((r) => r.reservationId === reservationId);
        if (!reservation) {
            throw new Error('Rezervacija nije pronađena.');
        }

        updater(reservation);
        const updated = [...reservations];
        saveReservations(updated);
        UserService.reservations.set(updated);
    }

    static updateReservationStatus(reservationId: string, status: ReservationStatus): void {
        UserService.updateReservation(reservationId, (reservation) => {
            reservation.status = status;
        });
    }

    static markReservationArrived(reservationId: string): void {
        UserService.updateReservation(reservationId, (reservation) => {
            if (reservation.status !== 'rezervisano') {
                throw new Error('Samo rezervisane igračke mogu biti označene kao pristigle.');
            }
            reservation.status = 'pristiglo';
        });
    }

    static deleteReservation(reservationId: string): void {
        const reservations = UserService.reservations().filter(
            (r) => r.reservationId !== reservationId
        );
        saveReservations(reservations);
        UserService.reservations.set(reservations);
    }

    static rateReservation(reservationId: string, rating: number): void {
        UserService.updateReservation(reservationId, (reservation) => {
            if (reservation.status !== 'pristiglo') {
                throw new Error('Ocenjivanje je moguće samo za rezervacije sa statusom "pristiglo".');
            }
            reservation.rating = rating;
        });
    }
}