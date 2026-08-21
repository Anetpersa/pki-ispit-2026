import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { UserService } from '../services/user.service';

export const authGuard: CanActivateFn = () => {
  const router = inject(Router);
  const user = UserService.getCurrentUser();

  if (user) {
    return true;
  }

  router.navigateByUrl('/login');
  return false;
};