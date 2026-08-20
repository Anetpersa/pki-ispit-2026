import { Component, OnInit, signal } from '@angular/core';
import { MatCardModule } from '@angular/material/card';
import { ToyModel } from '../../models/toy.model';
import { ToyService } from '../../services/toy.service';

@Component({
  selector: 'app-catalog',
  imports: [MatCardModule],
  templateUrl: './catalog.html',
  styleUrl: './catalog.css',
})
export class Catalog implements OnInit {
  toys = signal<ToyModel[]>([]);
  loading = signal(true);
  error = signal<string | null>(null);

  readonly imageBaseUrl = 'https://toy.pequla.com';

  ngOnInit() {
    this.loadToys();
  }

  async loadToys() {
    this.loading.set(true);
    this.error.set(null);
    try {
      const response = await ToyService.getToys();
      this.toys.set(response.data);
    } catch (err) {
      this.error.set('Ne mogu da učitam igračke. Pokušajte ponovo.');
    } finally {
      this.loading.set(false);
    }
  }
}