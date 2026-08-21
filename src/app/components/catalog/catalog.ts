import { Component, OnInit, computed, signal } from '@angular/core';
import { MatCardModule } from '@angular/material/card';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { FormsModule } from '@angular/forms';
import { ToyModel } from '../../../models/toy.model';
import { AgeGroupModel } from '../../../models/age-group.model';
import { ToyTypeModel } from '../../../models/toy-type.model';
import { ToyService } from '../../../services/toy.service';

@Component({
  selector: 'app-catalog',
  imports: [
    MatCardModule,
    MatCheckboxModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatButtonModule,
    FormsModule,
  ],
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

  readonly targetGroupOptions = [
    { value: '', label: 'Sve' },
    { value: 'svi', label: 'Svi' },
    { value: 'dečak', label: 'Dečak' },
    { value: 'devojčica', label: 'Devojčica' },
  ];

  searchTerm = signal('');
  selectedTypeIds = signal<Set<number>>(new Set());
  selectedAgeGroupIds = signal<Set<number>>(new Set());
  selectedTargetGroup = signal<string>('');
  minPrice = signal<number | null>(null);
  maxPrice = signal<number | null>(null);
  dateFrom = signal<string | null>(null);
  dateTo = signal<string | null>(null);

  filteredToys = computed(() => {
    const search = this.searchTerm().trim().toLowerCase();
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
      if (dFrom && new Date(toy.productionDate) < new Date(dFrom)) {
        return false;
      }
      if (dTo && new Date(toy.productionDate) > new Date(dTo)) {
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
      // filteri su sekundarni - ne blokiramo prikaz igračaka ako ovo padne
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
    this.searchTerm.set('');
    this.selectedTypeIds.set(new Set());
    this.selectedAgeGroupIds.set(new Set());
    this.selectedTargetGroup.set('');
    this.minPrice.set(null);
    this.maxPrice.set(null);
    this.dateFrom.set(null);
    this.dateTo.set(null);
  }
}