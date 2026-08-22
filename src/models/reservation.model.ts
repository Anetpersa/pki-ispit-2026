export type ReservationStatus = 'rezervisano' | 'pristiglo' | 'otkazano';

export interface ReservationModel {
  reservationId: string;
  userId: string;
  toyId: number;
  toyName: string;
  toyDescription: string;
  toyTypeName: string;
  toyAgeGroupName: string;
  toyTargetGroup: string;
  toyProductionDate: string;
  toyPrice: number;
  toyImageUrl: string;
  status: ReservationStatus;
  reservationDate: string;
  rating?: number;
}