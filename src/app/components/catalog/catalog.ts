import { Component, OnInit, computed } from '@angular/core';
import { RouterLink } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { provideNativeDateAdapter } from '@angular/material/core';
import { FormsModule } from '@angular/forms';
import { signal } from '@angular/core';
import { ToyModel } from '../../../models/toy.model';
import { AgeGroupModel } from '../../../models/age-group.model';
import { ToyTypeModel } from '../../../models/toy-type.model';
import { ToyService } from '../../../services/toy.service';
import { ReviewService } from '../../../services/review.service';
import { CatalogFilterService } from '../../../services/catalog-filter.service';
import { TARGET_GROUP_LABELS } from '../../../utils/target-group.util';

@Component({
  selector: 'app-catalog',
  imports: [
    RouterLink,
    MatCardModule,
    MatCheckboxModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatButtonModule,
    MatDatepickerModule,
    FormsModule,
  ],
  providers: [provideNativeDateAdapter()],
  templateUrl: './catalog.html',
  styleUrl: './catalog.css',
})
export class Catalog implements OnInit {
  allToys = signal<ToyModel[]>([]);
  ageGroups = signal<AgeGroupModel[]>([]);
  types = signal<ToyTypeModel[]>([]);
  loading = signal(true);
  error = signal<string | null>(null);

  readonly imageBaseUrl = 'https://toy.pequla.com';
  readonly minProductionDate = new Date(2000, 0, 1);
  readonly maxProductionDate = new Date();

  readonly targetGroupOptions = [
    { value: '', label: 'Sve ciljne grupe' },
    ...Object.entries(TARGET_GROUP_LABELS).map(([value, label]) => ({ value, label })),
  ];

  searchTerm = CatalogFilterService.searchTerm;
  reviewSearchTerm = CatalogFilterService.reviewSearchTerm;
  selectedTypeIds = CatalogFilterService.selectedTypeIds;
  selectedAgeGroupIds = CatalogFilterService.selectedAgeGroupIds;
  selectedTargetGroup = CatalogFilterService.selectedTargetGroup;
  minPrice = CatalogFilterService.minPrice;
  maxPrice = CatalogFilterService.maxPrice;
  dateFrom = CatalogFilterService.dateFrom;
  dateTo = CatalogFilterService.dateTo;

  filteredToys = computed(() => {
    const search = this.searchTerm().trim().toLowerCase();
    const reviewSearch = this.reviewSearchTerm().trim();
    const reviewToyIds = reviewSearch ? ReviewService.findToyIdsByReviewText(reviewSearch) : null;
    const typeIds = this.selectedTypeIds();
    const ageIds = this.selectedAgeGroupIds();
    const target = this.selectedTargetGroup();
    const minP = this.minPrice();
    const maxP = this.maxPrice();
    const dFrom = this.dateFrom();
    const dTo = this.dateTo();

    return this.allToys().filter((toy) => {
      if (
        search &&
        !toy.name.toLowerCase().includes(search) &&
        !toy.description.toLowerCase().includes(search)
      ) {
        return false;
      }
      if (reviewToyIds && !reviewToyIds.has(toy.toyId)) {
        return false;
      }
      if (typeIds.size > 0 && !typeIds.has(toy.type.typeId)) {
        return false;
      }
      if (ageIds.size > 0 && !ageIds.has(toy.ageGroup.ageGroupId)) {
        return false;
      }
      if (target && toy.targetGroup !== target) {
        return false;
      }
      if (minP != null && toy.price < minP) {
        return false;
      }
      if (maxP != null && toy.price > maxP) {
        return false;
      }
      if (dFrom && new Date(toy.productionDate).getTime() < dFrom.getTime()) {
        return false;
      }
      if (dTo && new Date(toy.productionDate).getTime() > dTo.getTime()) {
        return false;
      }
      return true;
    });
  });

  ngOnInit() {
    this.loadToys();
    this.loadFilters();
  }

  async loadToys() {
    this.loading.set(true);
    this.error.set(null);
    try {
      const response = await ToyService.getToys();
      this.allToys.set(response.data);
    } catch (err) {
      this.error.set('Ne mogu da učitam igračke. Pokušajte ponovo.');
    } finally {
      this.loading.set(false);
    }
  }

  async loadFilters() {
    try {
      const [ageGroupsRes, typesRes] = await Promise.all([
        ToyService.getAgeGroups(),
        ToyService.getTypes(),
      ]);
      this.ageGroups.set(ageGroupsRes.data);
      this.types.set(typesRes.data);
    } catch (err) {
      console.error('Greška pri učitavanju filtera', err);
    }
  }

  toggleType(typeId: number) {
    const current = new Set(this.selectedTypeIds());
    if (current.has(typeId)) {
      current.delete(typeId);
    } else {
      current.add(typeId);
    }
    this.selectedTypeIds.set(current);
  }

  toggleAgeGroup(ageGroupId: number) {
    const current = new Set(this.selectedAgeGroupIds());
    if (current.has(ageGroupId)) {
      current.delete(ageGroupId);
    } else {
      current.add(ageGroupId);
    }
    this.selectedAgeGroupIds.set(current);
  }

  clearFilters() {
    CatalogFilterService.clear();
  }
}