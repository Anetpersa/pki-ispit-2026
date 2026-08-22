import { Component, OnInit, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatChipsModule } from '@angular/material/chips';
import { MatIconModule } from '@angular/material/icon';
import { UserService } from '../../../services/user.service';
import { ToyService } from '../../../services/toy.service';
import { ToyTypeModel } from '../../../models/toy-type.model';

@Component({
  selector: 'app-profile',
  imports: [
    FormsModule,
    MatCardModule,
    MatFormFieldModule,
    MatInputModule,
    MatButtonModule,
    MatChipsModule,
    MatIconModule,
  ],
  templateUrl: './profile.html',
  styleUrl: './profile.css',
})
export class Profile implements OnInit {
  email = signal('');
  firstName = signal('');
  lastName = signal('');
  phone = signal('');
  address = signal('');
  selectedTypeIds = signal<Set<number>>(new Set());

  types = signal<ToyTypeModel[]>([]);
  loadingTypes = signal(true);

  saving = signal(false);
  saveMessage = signal<string | null>(null);
  errorMessage = signal<string | null>(null);

  private userId = '';

  ngOnInit() {
    const user = UserService.getCurrentUser();
    if (!user) {
      return;
    }

    this.userId = user.userId;
    this.email.set(user.email);
    this.firstName.set(user.firstName);
    this.lastName.set(user.lastName);
    this.phone.set(user.phone);
    this.address.set(user.address);
    this.selectedTypeIds.set(new Set(user.favoriteToyTypes ?? []));

    this.loadTypes();
  }

  async loadTypes() {
    this.loadingTypes.set(true);
    try {
      const response = await ToyService.getTypes();
      this.types.set(response.data);
    } catch (err) {
      console.error('Greška pri učitavanju tipova igračaka', err);
    } finally {
      this.loadingTypes.set(false);
    }
  }

  toggleFavoriteType(typeId: number) {
    const current = new Set(this.selectedTypeIds());
    if (current.has(typeId)) {
      current.delete(typeId);
    } else {
      current.add(typeId);
    }
    this.selectedTypeIds.set(current);
  }

  onSave() {
    this.errorMessage.set(null);
    this.saveMessage.set(null);
    this.saving.set(true);

    try {
      UserService.updateProfile(this.userId, {
        firstName: this.firstName(),
        lastName: this.lastName(),
        phone: this.phone(),
        address: this.address(),
        favoriteToyTypes: Array.from(this.selectedTypeIds()),
      });
      this.saveMessage.set('Podaci su uspešno sačuvani.');
    } catch (err) {
      this.errorMessage.set(err instanceof Error ? err.message : 'Čuvanje nije uspelo.');
    } finally {
      this.saving.set(false);
    }
  }
}