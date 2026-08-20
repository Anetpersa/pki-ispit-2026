import { Component, OnInit, signal } from '@angular/core';
import { ToyModel } from '../../models/toy.model';
import { ToyService } from '../../services/toy.service';

@Component({
  selector: 'app-home',
  imports: [],
  templateUrl: './home.html',
  styleUrl: './home.css',
})
export class Home implements OnInit {
  toys = signal<ToyModel[]>([]);
  loading = signal(true);
  error = signal<string | null>(null);

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