import { ToyModel } from '../models/toy.model';

export const TARGET_GROUP_LABELS: Record<ToyModel['targetGroup'], string> = {
  svi: 'Uniseks',
  dečak: 'Dečaci',
  devojčica: 'Devojčice',
};

export function targetGroupLabel(value: ToyModel['targetGroup'] | string): string {
  return (TARGET_GROUP_LABELS as Record<string, string>)[value] ?? value;
}