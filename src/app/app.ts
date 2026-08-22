import { Component, signal } from '@angular/core';
import { Router, RouterOutlet, RouterLink, RouterLinkActive } from '@angular/router';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatMenuModule } from '@angular/material/menu';
import { MatBadgeModule } from '@angular/material/badge';
import { UserService } from '../services/user.service';
import { CatalogFilterService } from '../services/catalog-filter.service';

@Component({
  selector: 'app-root',
  imports: [
    RouterOutlet,
    RouterLink,
    RouterLinkActive,
    MatToolbarModule,
    MatButtonModule,
    MatIconModule,
    MatMenuModule,
    MatBadgeModule,
  ],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {
  protected readonly title = signal('pki-ispit-2026');
  protected readonly currentUser = UserService.currentUser;
  protected readonly cartCount = UserService.activeCartCount;

  constructor(private router: Router) {}

  logout() {
    UserService.logout();
    CatalogFilterService.clear();
    this.router.navigateByUrl('/');
  }

  onSearch(term: string) {
    CatalogFilterService.searchTerm.set(term.trim());
    this.router.navigateByUrl('/igracke');
  }
}