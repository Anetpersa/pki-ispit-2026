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
  selector: 'app-login',
  imports: [
    FormsModule,
    RouterLink,
    MatCardModule,
    MatFormFieldModule,
    MatInputModule,
    MatButtonModule,
    MatIconModule,
  ],
  templateUrl: './login.html',
  styleUrl: './login.css',
})
export class Login {
  email = signal('');
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
      await UserService.login(this.email(), this.password());
      this.router.navigateByUrl(this.returnUrl ?? '/');
    } catch (err) {
      this.error.set(err instanceof Error ? err.message : 'Prijava nije uspela.');
    } finally {
      this.loading.set(false);
    }
  }
}