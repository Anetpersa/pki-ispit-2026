import { signal } from '@angular/core';

export class CatalogFilterService {
  static searchTerm = signal('');
  static reviewSearchTerm = signal('');
  static selectedTypeIds = signal<Set<number>>(new Set());
  static selectedAgeGroupIds = signal<Set<number>>(new Set());
  static selectedTargetGroup = signal<string>('');
  static minPrice = signal<number | null>(null);
  static maxPrice = signal<number | null>(null);
  static dateFrom = signal<Date | null>(null);
  static dateTo = signal<Date | null>(null);

  static clear(): void {
    CatalogFilterService.searchTerm.set('');
    CatalogFilterService.reviewSearchTerm.set('');
    CatalogFilterService.selectedTypeIds.set(new Set());
    CatalogFilterService.selectedAgeGroupIds.set(new Set());
    CatalogFilterService.selectedTargetGroup.set('');
    CatalogFilterService.minPrice.set(null);
    CatalogFilterService.maxPrice.set(null);
    CatalogFilterService.dateFrom.set(null);
    CatalogFilterService.dateTo.set(null);
  }
}