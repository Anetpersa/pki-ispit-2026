import { Component, OnInit, signal } from '@angular/core';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { Location } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatChipsModule } from '@angular/material/chips';
import { UserService } from '../../../services/user.service';
import { ToyService } from '../../../services/toy.service';
import { ToyTypeModel } from '../../../models/toy-type.model';

@Component({
  selector: 'app-signup',
  imports: [
    FormsModule,
    RouterLink,
    MatCardModule,
    MatFormFieldModule,
    MatInputModule,
    MatButtonModule,
    MatIconModule,
    MatChipsModule,
  ],
  templateUrl: './signup.html',
  styleUrl: './signup.css',
})
export class Signup implements OnInit {
  firstName = signal('');
  lastName = signal('');
  email = signal('');
  phone = signal('');
  address = signal('');
  password = signal('');
  showPassword = signal(false);
  error = signal<string | null>(null);
  loading = signal(false);
  returnUrl: string | null = null;

  types = signal<ToyTypeModel[]>([]);
  loadingTypes = signal(true);
  selectedTypeIds = signal<Set<number>>(new Set());

  constructor(
    private router: Router,
    private route: ActivatedRoute,
    private location: Location
  ) {
    this.returnUrl = this.route.snapshot.queryParamMap.get('returnUrl');
  }

  ngOnInit() {
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

  goBack() {
    this.location.back();
  }

  toggleShowPassword() {
    this.showPassword.set(!this.showPassword());
  }

  async onSubmit() {
    this.error.set(null);
    this.loading.set(true);
    try {
      await UserService.signup(
        this.email(),
        this.password(),
        this.firstName(),
        this.lastName(),
        this.phone(),
        this.address(),
        Array.from(this.selectedTypeIds())
      );
      this.router.navigateByUrl(this.returnUrl ?? '/');
    } catch (err) {
      this.error.set(err instanceof Error ? err.message : 'Registracija nije uspela.');
    } finally {
      this.loading.set(false);
    }
  }
}