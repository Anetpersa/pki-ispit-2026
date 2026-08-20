import axios from 'axios';
import { ToyModel } from '../models/toy.model';
import { AgeGroupModel } from '../models/age-group.model';
import { ToyTypeModel } from '../models/toy-type.model';

const client = axios.create({
  baseURL: 'https://toy.pequla.com/api',
  validateStatus: (status: number) => status === 200,
  headers: {
    Accept: 'application/json',
  },
});

export class ToyService {
  static async getToys() {
    return await client.get<ToyModel[]>('/toy');
  }

  static async getToyById(id: number) {
    return await client.get<ToyModel>(`/toy/${id}`);
  }

  static async getToyByPermalink(permalink: string) {
    return await client.get<ToyModel>(`/toy/permalink/${permalink}`);
  }

  static async getToysByIds(ids: number[]) {
    return await client.request<ToyModel[]>({
      url: '/toy/list',
      method: 'post',
      data: ids,
    });
  }

  static async getAgeGroups() {
    return await client.get<AgeGroupModel[]>('/age-group');
  }

  static async getTypes() {
    return await client.get<ToyTypeModel[]>('/type');
  }
}