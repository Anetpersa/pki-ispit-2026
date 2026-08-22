import { Component, signal } from '@angular/core';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { Location } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { UserService } from '../../../services/user.service';

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
  ],
  templateUrl: './signup.html',
  styleUrl: './signup.css',
})
export class Signup {
  firstName = signal('');
  lastName = signal('');
  email = signal('');
  phone = signal('');
  address = signal('');
  password = signal('');
  error = signal<string | null>(null);
  loading = signal(false);
  returnUrl: string | null = null;

  constructor(
    private router: Router,
    private route: ActivatedRoute,
    private location: Location
  ) {
    this.returnUrl = this.route.snapshot.queryParamMap.get('returnUrl');
  }

  goBack() {
    this.location.back();
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
        this.address()
      );
      this.router.navigateByUrl(this.returnUrl ?? '/');
    } catch (err) {
      this.error.set(err instanceof Error ? err.message : 'Registracija nije uspela.');
    } finally {
      this.loading.set(false);
    }
  }
}