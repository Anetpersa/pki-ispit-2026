export type ReservationStatus = 'rezervisano' | 'pristiglo' | 'otkazano';

export interface ReservationModel {
  reservationId: string;
  userId: string;
  toyId: number;
  toyName: string;
  toyPrice: number;
  toyImageUrl: string;
  status: ReservationStatus;
  reservationDate: string;
  rating?: number;
}