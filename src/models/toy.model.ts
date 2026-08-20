import { AgeGroupModel } from './age-group.model';
import { ToyTypeModel } from './toy-type.model';

export interface ToyModel {
  toyId: number;
  name: string;
  permalink: string;
  description: string;
  targetGroup: 'svi' | 'dečak' | 'devojčica';
  productionDate: string;
  price: number;
  imageUrl: string;
  ageGroup: AgeGroupModel;
  type: ToyTypeModel;
}