import { Component, OnInit, signal } from '@angular/core';
import { RouterLink } from '@angular/router';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { ToyTypeModel } from '../../models/toy-type.model';
import { ToyService } from '../../services/toy.service';

interface CategoryDisplay {
  typeId: number;
  name: string;
  icon: string;
}

const TYPE_ICONS: Record<number, string> = {
  1: 'extension',
  2: 'menu_book',
  3: 'accessibility_new',
  4: 'palette',
  5: 'directions_car',
  6: 'pets',
  7: 'casino',
  8: 'construction',
  9: 'music_note',
  10: 'school',
};

@Component({
  selector: 'app-home',
  imports: [RouterLink, MatButtonModule, MatCardModule, MatIconModule],
  templateUrl: './home.html',
  styleUrl: './home.css',
})
export class Home implements OnInit {
  categories = signal<CategoryDisplay[]>([]);

  ngOnInit() {
    this.loadCategories();
  }

  async loadCategories() {
    try {
      const response = await ToyService.getTypes();
      this.categories.set(
        response.data.map((t: ToyTypeModel) => ({
          typeId: t.typeId,
          name: t.name,
          icon: TYPE_ICONS[t.typeId] ?? 'toys',
        }))
      );
    } catch {
      this.categories.set([]);
    }
  }
}