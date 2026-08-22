import { Component, signal } from '@angular/core';
import { NavigationEnd, Router, RouterOutlet, RouterLink, RouterLinkActive } from '@angular/router';
import { filter } from 'rxjs/operators';
import { FormsModule } from '@angular/forms';
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
    FormsModule,
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
  protected readonly searchTerm = CatalogFilterService.searchTerm;
  protected readonly mobileSearchOpen = signal(false);

  constructor(private router: Router) {
    this.router.events
      .pipe(filter((event): event is NavigationEnd => event instanceof NavigationEnd))
      .subscribe((event) => {
        if (!event.urlAfterRedirects.startsWith('/igracke')) {
          CatalogFilterService.clear();
        }
        this.mobileSearchOpen.set(false);
      });
  }

  toggleMobileSearch() {
    this.mobileSearchOpen.set(!this.mobileSearchOpen());
  }

  closeMobileSearch() {
    this.mobileSearchOpen.set(false);
  }

  logout() {
    UserService.logout();
    CatalogFilterService.clear();
    this.router.navigateByUrl('/');
  }

  onSearch() {
    this.router.navigateByUrl('/igracke');
    this.closeMobileSearch();
  }

  clearSearch() {
    CatalogFilterService.searchTerm.set('');
  }
}