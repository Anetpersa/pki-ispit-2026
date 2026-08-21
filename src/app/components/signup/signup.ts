import { Component, signal } from '@angular/core';
import { Router, RouterLink } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
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
  ],
  templateUrl: './signup.html',
  styleUrl: './signup.css',
})
export class Signup {
  firstName = signal('');
  lastName = signal('');
  email = signal('');
  password = signal('');
  error = signal<string | null>(null);
  loading = signal(false);

  constructor(private router: Router) {}

  async onSubmit() {
    this.error.set(null);
    this.loading.set(true);
    try {
      await UserService.signup(this.email(), this.password(), this.firstName(), this.lastName());
      this.router.navigateByUrl('/');
    } catch (err) {
      this.error.set(err instanceof Error ? err.message : 'Registracija nije uspela.');
    } finally {
      this.loading.set(false);
    }
  }
}