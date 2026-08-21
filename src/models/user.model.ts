export interface UserModel {
  userId: string;
  email: string;
  passwordHash: string;
  firstName: string;
  lastName: string;
  phone: string;
  address: string;
  favoriteToyTypes: number[];
}